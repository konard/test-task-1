"""Deterministic task scheduler simulator for a distributed system.

The :func:`run_simulation` function takes a JSON-compatible ``dict`` describing
a cluster, a set of tasks, and a list of timed events, and produces a
deterministic ``dict`` describing how the simulation unfolded.

See ``docs/SCHEDULER.md`` for the full architecture description, complexity
analysis, edge cases, and limitations.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


# --- Status constants -------------------------------------------------------

STATUS_PENDING = "pending"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_BLOCKED = "blocked"
STATUS_DEDUPLICATED = "deduplicated"
STATUS_IDEMPOTENCY_CONFLICT = "idempotency_conflict"


# Internal-only state distinguishing tasks that are actively running.
_INTERNAL_RUNNING = "_running"


# --- Exceptions -------------------------------------------------------------


class SchedulerError(ValueError):
    """Base class for scheduler input errors."""


class DependencyCycleError(SchedulerError):
    """Raised when the dependency graph contains a cycle."""


# --- Data classes -----------------------------------------------------------


@dataclass
class _Task:
    id: str
    duration_ms: int
    cpu: int
    ram: int
    gpu: int
    priority: int
    deadline_ms: int
    depends_on: list[str]
    retryable: bool
    max_retries: int
    failures: list[dict[str, int]]
    idempotency_key: str | None

    # Mutable runtime state.
    status: str = STATUS_PENDING
    attempts: int = 0
    started_at: int | None = None
    finished_at: int | None = None
    worker_id: str | None = None
    failure_reason: str | None = None
    # Set only while running.
    _attempt_start: int | None = None
    _attempt_fail_at: int | None = None


@dataclass
class _Worker:
    id: str
    cpu: int
    ram: int
    gpu: int
    local_rate_limit_per_sec: int
    clock_skew_ms: int
    offline_windows: list[list[int]]
    cpu_used: int = 0
    ram_used: int = 0
    gpu_used: int = 0
    online: bool = True
    # Sorted list of start timestamps (ms) used for the rolling rate window.
    start_log: list[int] = field(default_factory=list)


# --- Helpers ----------------------------------------------------------------


def _ordering_key(task: _Task) -> tuple[int, int, str]:
    """Deterministic ordering: higher priority, smaller deadline, smaller id."""
    return (-task.priority, task.deadline_ms, task.id)


def _is_offline_at(worker: _Worker, time_ms: int) -> bool:
    """Return ``True`` when ``time_ms`` falls inside any offline window.

    Offline windows are inclusive of start, exclusive of end so that a window
    ``[a, b]`` represents ``[a, b)``.
    """
    for start, end in worker.offline_windows:
        if start <= time_ms < end:
            return True
    return False


def _within_rate(log: list[int], time_ms: int, limit: int) -> bool:
    """Check whether starting at ``time_ms`` would violate the per-second
    sliding-window rate ``limit``.
    """
    if limit <= 0:
        return False
    # Window is the last 1000 ms inclusive of ``time_ms``.
    cutoff = time_ms - 999
    count = sum(1 for t in log if t >= cutoff)
    return count < limit


def _normalize_failures(raw: Any) -> list[dict[str, int]]:
    if not raw:
        return []
    out: list[dict[str, int]] = []
    for entry in raw:
        out.append(
            {
                "attempt": int(entry["attempt"]),
                "fail_at_ms": int(entry["fail_at_ms"]),
            }
        )
    return out


def _task_from_dict(data: dict[str, Any]) -> _Task:
    return _Task(
        id=str(data["id"]),
        duration_ms=int(data["duration_ms"]),
        cpu=int(data.get("cpu", 0)),
        ram=int(data.get("ram", 0)),
        gpu=int(data.get("gpu", 0)),
        priority=int(data.get("priority", 1)),
        deadline_ms=int(data.get("deadline_ms", 0)),
        depends_on=list(data.get("depends_on", []) or []),
        retryable=bool(data.get("retryable", False)),
        max_retries=int(data.get("max_retries", 0)),
        failures=_normalize_failures(data.get("failures")),
        idempotency_key=(
            str(data["idempotency_key"]) if data.get("idempotency_key") else None
        ),
    )


def _worker_from_dict(data: dict[str, Any]) -> _Worker:
    windows_raw = data.get("offline_windows", []) or []
    windows: list[list[int]] = []
    for w in windows_raw:
        if len(w) != 2:
            msg = f"worker {data.get('id')!r} has malformed offline window {w!r}"
            raise SchedulerError(msg)
        windows.append([int(w[0]), int(w[1])])
    return _Worker(
        id=str(data["id"]),
        cpu=int(data.get("cpu", 0)),
        ram=int(data.get("ram", 0)),
        gpu=int(data.get("gpu", 0)),
        local_rate_limit_per_sec=int(data.get("local_rate_limit_per_sec", 0)),
        clock_skew_ms=int(data.get("clock_skew_ms", 0)),
        offline_windows=windows,
    )


def _idempotency_signature(task: _Task) -> tuple[Any, ...]:
    """Return a hashable signature describing the immutable parameters of a
    task for the purposes of idempotency equivalence checks.
    """
    return (
        task.duration_ms,
        task.cpu,
        task.ram,
        task.gpu,
        task.priority,
        task.deadline_ms,
        tuple(task.depends_on),
        task.retryable,
        task.max_retries,
        tuple((f["attempt"], f["fail_at_ms"]) for f in task.failures),
    )


def _detect_cycle(tasks: dict[str, _Task]) -> None:
    """Run Kahn's algorithm over the static dependency graph and raise
    :class:`DependencyCycleError` if a cycle exists.
    """
    in_degree: dict[str, int] = {tid: 0 for tid in tasks}
    children: dict[str, list[str]] = {tid: [] for tid in tasks}
    for tid, task in tasks.items():
        for dep in task.depends_on:
            if dep not in tasks:
                # Missing dependency cannot form a cycle, but we want to keep
                # the structural information for blocking decisions later.
                continue
            in_degree[tid] += 1
            children[dep].append(tid)

    queue = deque(sorted(tid for tid, d in in_degree.items() if d == 0))
    visited = 0
    while queue:
        tid = queue.popleft()
        visited += 1
        for child in sorted(children[tid]):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if visited != len(tasks):
        msg = "dependency cycle detected in task graph"
        raise DependencyCycleError(msg)


# --- Main entry point -------------------------------------------------------


def run_simulation(input_json: dict[str, Any]) -> dict[str, Any]:
    """Run the deterministic scheduler simulation.

    Args:
        input_json: A JSON-compatible ``dict`` matching the schema described
            in the issue.

    Returns:
        A JSON-compatible ``dict`` containing per-task results, an event
        log, per-worker logs, and aggregated metrics.

    Raises:
        DependencyCycleError: If the static dependency graph contains a cycle.
        SchedulerError: For other malformed input.
    """
    sim = input_json.get("simulation", {})
    start_time = int(sim.get("start_time", 0))
    end_time = int(sim.get("end_time", 0))
    tick_ms = int(sim.get("tick_ms", 100))
    if tick_ms <= 0:
        msg = "simulation.tick_ms must be positive"
        raise SchedulerError(msg)
    global_rate = int(sim.get("global_rate_limit_per_sec", 0))

    workers: dict[str, _Worker] = {}
    for w in input_json.get("workers", []) or []:
        worker = _worker_from_dict(w)
        if worker.id in workers:
            msg = f"duplicate worker id {worker.id!r}"
            raise SchedulerError(msg)
        workers[worker.id] = worker

    tasks: dict[str, _Task] = {}
    for t in input_json.get("tasks", []) or []:
        task = _task_from_dict(t)
        if task.id in tasks:
            msg = f"duplicate task id {task.id!r}"
            raise SchedulerError(msg)
        tasks[task.id] = task

    _detect_cycle(tasks)

    # Pre-index events by their tick time. ``add_task`` events introduce new
    # tasks; ``cancel_task`` events mark targets for cancellation.
    events_by_time: dict[int, list[dict[str, Any]]] = {}
    for ev in input_json.get("events", []) or []:
        t = int(ev["time_ms"])
        events_by_time.setdefault(t, []).append(ev)

    events_log: list[dict[str, Any]] = []
    worker_log: dict[str, list[dict[str, Any]]] = {wid: [] for wid in workers}
    global_start_log: list[int] = []

    # Track per-key currently-running tasks and the most recent successful
    # signature so we can implement idempotency rules.
    running_idempotency: dict[str, str] = {}  # key -> task_id currently running
    completed_idempotency: dict[str, tuple[Any, ...]] = {}  # key -> signature

    def log_event(time_ms: int, etype: str, **fields: Any) -> None:
        entry: dict[str, Any] = {"time_ms": time_ms, "type": etype}
        entry.update(fields)
        events_log.append(entry)

    def log_worker(worker: _Worker, time_ms: int, etype: str, **fields: Any) -> None:
        entry: dict[str, Any] = {
            "worker_time_ms": time_ms + worker.clock_skew_ms,
            "global_time_ms": time_ms,
            "type": etype,
        }
        entry.update(fields)
        worker_log[worker.id].append(entry)

    def release_resources(worker: _Worker, task: _Task) -> None:
        worker.cpu_used -= task.cpu
        worker.ram_used -= task.ram
        worker.gpu_used -= task.gpu

    def deps_state(task: _Task) -> str:
        """Return ``"ready"``, ``"waiting"``, or ``"blocked"`` based on the
        statuses of dependencies. ``deduplicated`` is treated as
        success-equivalent because the keyed work is known to have already
        completed elsewhere.
        """
        for dep in task.depends_on:
            dep_task = tasks.get(dep)
            if dep_task is None:
                return STATUS_BLOCKED
            if dep_task.status in (
                STATUS_FAILED,
                STATUS_CANCELLED,
                STATUS_BLOCKED,
                STATUS_IDEMPOTENCY_CONFLICT,
            ):
                return STATUS_BLOCKED
            if dep_task.status not in (STATUS_SUCCESS, STATUS_DEDUPLICATED):
                return "waiting"
        return "ready"

    def cancel_running(task: _Task, time_ms: int, reason: str) -> None:
        worker = workers[task.worker_id] if task.worker_id else None
        if worker is not None:
            release_resources(worker, task)
            log_worker(worker, time_ms, "task_cancelled", task_id=task.id)
        if task.idempotency_key is not None:
            running_idempotency.pop(task.idempotency_key, None)
        task.status = STATUS_CANCELLED
        task.finished_at = time_ms
        task.failure_reason = reason
        task._attempt_start = None
        task._attempt_fail_at = None

    def fail_attempt(task: _Task, time_ms: int, reason: str) -> bool:
        """Handle a single failed attempt. Returns ``True`` if the task is
        retried, ``False`` if it has terminally failed.
        """
        worker = workers[task.worker_id] if task.worker_id else None
        if worker is not None:
            release_resources(worker, task)
            log_worker(worker, time_ms, "task_failed_attempt", task_id=task.id)
        if task.idempotency_key is not None:
            running_idempotency.pop(task.idempotency_key, None)
        log_event(
            time_ms,
            "task_failed_attempt",
            task_id=task.id,
            worker_id=task.worker_id,
            reason=reason,
        )
        task._attempt_start = None
        task._attempt_fail_at = None
        task.worker_id = None
        if task.retryable and task.attempts <= task.max_retries:
            task.status = STATUS_PENDING
            return True
        task.status = STATUS_FAILED
        task.finished_at = time_ms
        task.failure_reason = reason
        return False

    def finish_running(task: _Task, time_ms: int) -> None:
        worker = workers[task.worker_id] if task.worker_id else None
        if worker is not None:
            release_resources(worker, task)
            log_worker(worker, time_ms, "task_finished", task_id=task.id)
        log_event(
            time_ms,
            "task_finished",
            task_id=task.id,
            worker_id=task.worker_id,
        )
        if task.idempotency_key is not None:
            running_idempotency.pop(task.idempotency_key, None)
            completed_idempotency[task.idempotency_key] = _idempotency_signature(task)
        task.status = STATUS_SUCCESS
        task.finished_at = time_ms
        task._attempt_start = None
        task._attempt_fail_at = None

    def try_start(task: _Task, time_ms: int) -> bool:
        """Attempt to start ``task`` at ``time_ms`` on the best fitting
        worker. Returns ``True`` on success.
        """
        # Idempotency: deduplicate / conflict / wait-for-running.
        if task.idempotency_key is not None:
            key = task.idempotency_key
            if key in running_idempotency:
                # Another task with this key is currently running; wait.
                return False
            if key in completed_idempotency:
                if completed_idempotency[key] == _idempotency_signature(task):
                    task.status = STATUS_DEDUPLICATED
                    task.finished_at = time_ms
                    log_event(time_ms, "task_deduplicated", task_id=task.id)
                else:
                    task.status = STATUS_IDEMPOTENCY_CONFLICT
                    task.finished_at = time_ms
                    log_event(time_ms, "task_idempotency_conflict", task_id=task.id)
                return False

        if not _within_rate(global_start_log, time_ms, global_rate):
            return False

        # Deterministically pick the lowest-id worker that fits.
        for wid in sorted(workers):
            worker = workers[wid]
            if not worker.online:
                continue
            if worker.cpu - worker.cpu_used < task.cpu:
                continue
            if worker.ram - worker.ram_used < task.ram:
                continue
            if worker.gpu - worker.gpu_used < task.gpu:
                continue
            if not _within_rate(
                worker.start_log, time_ms, worker.local_rate_limit_per_sec
            ):
                continue

            # Reserve resources.
            worker.cpu_used += task.cpu
            worker.ram_used += task.ram
            worker.gpu_used += task.gpu
            worker.start_log.append(time_ms)
            global_start_log.append(time_ms)
            task.attempts += 1
            task.status = _INTERNAL_RUNNING
            task.worker_id = worker.id
            if task.started_at is None:
                task.started_at = time_ms
            task._attempt_start = time_ms
            # Find scheduled failure for this attempt.
            task._attempt_fail_at = None
            for f in task.failures:
                if f["attempt"] == task.attempts:
                    task._attempt_fail_at = time_ms + f["fail_at_ms"]
                    break
            if task.idempotency_key is not None:
                running_idempotency[task.idempotency_key] = task.id
            log_event(time_ms, "task_started", task_id=task.id, worker_id=worker.id)
            log_worker(worker, time_ms, "task_started", task_id=task.id)
            return True
        return False

    def add_task_at(time_ms: int, task_dict: dict[str, Any]) -> None:
        new_task = _task_from_dict(task_dict)
        if new_task.id in tasks:
            log_event(
                time_ms,
                "add_task_ignored",
                task_id=new_task.id,
                reason="duplicate_id",
            )
            return
        tasks[new_task.id] = new_task
        # Re-validate the cycle invariant including the new task.
        try:
            _detect_cycle(tasks)
        except DependencyCycleError:
            del tasks[new_task.id]
            log_event(
                time_ms,
                "add_task_ignored",
                task_id=new_task.id,
                reason="dependency_cycle",
            )
            return
        log_event(time_ms, "task_added", task_id=new_task.id)

    def process_cancel(time_ms: int, task_id: str) -> None:
        task = tasks.get(task_id)
        if task is None:
            log_event(
                time_ms,
                "cancel_ignored",
                task_id=task_id,
                reason="unknown_task",
            )
            return
        if task.status == _INTERNAL_RUNNING:
            cancel_running(task, time_ms, "cancelled_by_event")
            log_event(
                time_ms, "task_cancelled", task_id=task_id, worker_id=task.worker_id
            )
            return
        if task.status in (STATUS_PENDING, STATUS_BLOCKED):
            task.status = STATUS_CANCELLED
            task.finished_at = time_ms
            task.failure_reason = "cancelled_by_event"
            log_event(time_ms, "task_cancelled", task_id=task_id)
            return
        # Already terminal: log but ignore.
        log_event(
            time_ms,
            "cancel_ignored",
            task_id=task_id,
            reason=f"already_{task.status}",
        )

    def update_offline_state(time_ms: int) -> None:
        for worker in workers.values():
            should_be_offline = _is_offline_at(worker, time_ms)
            if should_be_offline and worker.online:
                worker.online = False
                log_worker(worker, time_ms, "worker_offline")
                log_event(time_ms, "worker_offline", worker_id=worker.id)
                # Kill running tasks on this worker.
                for task in list(tasks.values()):
                    if task.status == _INTERNAL_RUNNING and task.worker_id == worker.id:
                        fail_attempt(task, time_ms, "worker_offline")
            elif not should_be_offline and not worker.online:
                worker.online = True
                log_worker(worker, time_ms, "worker_online")
                log_event(time_ms, "worker_online", worker_id=worker.id)

    # --- Tick loop ---------------------------------------------------------

    time_ms = start_time
    while time_ms <= end_time:
        # 1. Apply discrete events scheduled exactly at this tick.
        for ev in events_by_time.get(time_ms, []):
            etype = ev.get("type")
            if etype == "add_task":
                add_task_at(time_ms, ev["task"])
            elif etype == "cancel_task":
                process_cancel(time_ms, str(ev["task_id"]))
            else:
                log_event(time_ms, "event_ignored", details=ev)

        # 2. Apply offline-window transitions (after events so newly added
        #    tasks see the current state).
        update_offline_state(time_ms)

        # 3. Resolve task completions and scheduled failures.
        for task in list(tasks.values()):
            if task.status != _INTERNAL_RUNNING:
                continue
            assert task._attempt_start is not None
            if task._attempt_fail_at is not None and time_ms >= task._attempt_fail_at:
                fail_attempt(task, time_ms, "scripted_failure")
                continue
            if time_ms >= task._attempt_start + task.duration_ms:
                finish_running(task, time_ms)

        # 4. Mark tasks that are unrecoverably blocked.
        for task in tasks.values():
            if task.status == STATUS_PENDING:
                state = deps_state(task)
                if state == STATUS_BLOCKED:
                    task.status = STATUS_BLOCKED
                    task.finished_at = time_ms
                    log_event(time_ms, "task_blocked", task_id=task.id)

        # 5. Try to start ready tasks in deterministic priority order.
        ready = [
            t
            for t in tasks.values()
            if t.status == STATUS_PENDING and deps_state(t) == "ready"
        ]
        ready.sort(key=_ordering_key)
        for task in ready:
            if not _within_rate(global_start_log, time_ms, global_rate):
                break
            try_start(task, time_ms)

        time_ms += tick_ms

    # --- Build output ------------------------------------------------------

    final_tasks: dict[str, dict[str, Any]] = {}
    counts: dict[str, int] = {
        STATUS_SUCCESS: 0,
        STATUS_FAILED: 0,
        STATUS_CANCELLED: 0,
        STATUS_BLOCKED: 0,
        STATUS_DEDUPLICATED: 0,
        STATUS_IDEMPOTENCY_CONFLICT: 0,
        STATUS_PENDING: 0,
        "deadline_missed": 0,
        "total_attempts": 0,
    }
    utilization: dict[str, int] = {"cpu_time": 0, "ram_time": 0, "gpu_time": 0}

    for tid, task in tasks.items():
        # Anything still running or pending at the end of the simulation is
        # reported as ``pending`` per the spec.
        public_status = task.status
        if public_status == _INTERNAL_RUNNING:
            public_status = STATUS_PENDING

        # Compute deadline_missed.
        if public_status == STATUS_SUCCESS:
            deadline_missed = (
                task.finished_at is not None and task.finished_at > task.deadline_ms
            )
        elif public_status in (
            STATUS_DEDUPLICATED,
            STATUS_IDEMPOTENCY_CONFLICT,
        ):
            deadline_missed = False
        else:
            deadline_missed = end_time > task.deadline_ms

        final_tasks[tid] = {
            "status": public_status,
            "attempts": task.attempts,
            "started_at": task.started_at,
            "finished_at": task.finished_at,
            "worker_id": task.worker_id,
            "deadline_missed": bool(deadline_missed),
            "failure_reason": task.failure_reason,
        }

        counts[public_status] += 1
        if deadline_missed:
            counts["deadline_missed"] += 1
        counts["total_attempts"] += task.attempts

        # Approximate utilization: duration_ms * resources for successful
        # tasks. Failed/cancelled attempts also consume resources but the
        # spec only requires a representative figure.
        if public_status == STATUS_SUCCESS and task.started_at is not None:
            elapsed = (task.finished_at or task.started_at) - task.started_at
            utilization["cpu_time"] += task.cpu * elapsed
            utilization["ram_time"] += task.ram * elapsed
            utilization["gpu_time"] += task.gpu * elapsed

    metrics: dict[str, Any] = dict(counts)
    metrics["resource_utilization"] = utilization

    return {
        "tasks": final_tasks,
        "events_log": events_log,
        "worker_log": worker_log,
        "metrics": metrics,
    }
