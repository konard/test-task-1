# Deterministic Distributed Task Scheduler — Design Notes

This document accompanies `src/my_package/scheduler.py` and explains the
architecture, complexity, edge cases handled, ambiguities resolved, and known
limitations of the implementation.

## 1. Architecture

The scheduler is a single-threaded, **discrete-event simulator** with
millisecond-precision time semantics. The whole simulation is implemented
in pure Python with no external dependencies (only `collections`,
`dataclasses`, `heapq`, and `typing` from the standard library).

### 1.1 Core entities

* **`_Task`** — runtime state for a task: status, attempt counter, start /
  finish timestamps, the worker it is currently running on, and the
  scripted-failure timestamp for the active attempt.
* **`_Worker`** — runtime state for a worker: capacity, current resource
  usage, online flag, sliding-window start log for rate-limit checks, and
  static config (skew, offline windows, capacity).

### 1.2 Time advance and per-moment pipeline

Time advances to the next *significant moment* drawn from a min-heap that
contains:

* every periodic re-evaluation tick at `start_time + k · tick_ms`,
* every external event time (exact ms),
* every offline-window edge (start and end), exact ms,
* every dynamic completion / scripted-failure time of a running task,
  exact ms,
* every rate-limit window expiration `start + 1000 ms` so a deferred
  start is retried at the precise ms the limit lifts.

`tick_ms` therefore acts purely as a periodic re-evaluation cadence (so a
task that could not start because of a saturated rate window or a
resource conflict gets retried even when no other event fires); it never
snaps event, completion, or scripted-failure semantics to a tick
boundary. As a consequence, the same scenario produces identical task
outcomes for any reasonable `tick_ms` (covered by
`test_independent_of_tick_ms_value`).

For every visited moment `t` the simulator performs the following
deterministic steps:

1. **Apply discrete events** scheduled exactly at `t`
   (`add_task`, `cancel_task`). An `add_task` event fires at its declared
   `time_ms`, even when that ms is not a multiple of `tick_ms`.
2. **At an offline-window-start moment**, *first* resolve completions and
   scripted failures that were due at or before `t`. This means a task
   whose natural `attempt_start + duration_ms` lands exactly on the ms
   the worker goes offline succeeds instead of being failed by the
   offline transition.
3. **Update worker online/offline state.** When a worker transitions from
   online → offline, every task still running on it is terminated as a
   `failed_attempt` (with reason `worker_offline`).
4. **Resolve completions and scripted failures.** Running tasks whose
   scripted failure time is reached fail; otherwise running tasks whose
   `attempt_start + duration_ms` has elapsed succeed.
5. **Mark unrecoverably blocked tasks.** A pending task whose dependency
   set contains any failed/cancelled/blocked/idempotency-conflict task is
   transitioned to `blocked`.
6. **Schedule new starts.** Pending tasks with all dependencies satisfied
   are sorted using the canonical priority key
   `(-priority, deadline_ms, id)` and offered the workers in ascending
   `worker_id` order. A start is admitted only when CPU/RAM/GPU fit and
   both rate windows allow it. Successful starts enqueue their natural
   completion and scripted-failure moments so the simulator wakes up at
   the precise ms.

### 1.3 Determinism guarantees

* Ordering of ready tasks is fully determined by the tuple
  `(-priority, deadline_ms, id)` — no Python dict iteration, no `random`.
* Worker selection iterates `sorted(workers)`.
* Cycle detection uses Kahn's algorithm with sorted children, also
  deterministic.
* Completions are detected by comparing `time_ms >= attempt_start +
  duration_ms`, so successive ticks of the same input always produce the
  same global timeline.

### 1.4 Idempotency model

Idempotency is implemented with two registries:

* `running_idempotency: dict[key, task_id]` — set on every successful
  start, cleared on completion / failure / cancellation.
* `completed_idempotency: dict[key, signature]` — set when a task with
  that key reaches `success`, with the canonical hashable signature
  derived from the task's immutable parameters
  (`duration_ms`, `cpu`, `ram`, `gpu`, `priority`, `deadline_ms`,
  `depends_on`, `retryable`, `max_retries`, `failures`).

A task with an idempotency key is treated as follows whenever it is about
to be started:

1. If another task with the same key is currently running → defer.
2. Else if a previous task with the same key has `success`:
    * If signatures match → status `deduplicated` (no execution).
    * Otherwise → status `idempotency_conflict`.
3. Else → start as usual.

### 1.5 Output schema

The result mirrors the schema given in the issue:

* `tasks` — a `dict[task_id, {...}]` containing `status`, `attempts`,
  `started_at`, `finished_at`, `worker_id`, `deadline_missed`,
  `failure_reason`.
* `events_log` — chronological list of global events
  (`task_started`, `task_finished`, `task_failed_attempt`,
  `task_cancelled`, `task_blocked`, `task_added`, `task_deduplicated`,
  `task_idempotency_conflict`, `worker_online`, `worker_offline`,
  `cancel_ignored`, `add_task_ignored`).
* `worker_log` — per-worker chronological log including the
  skew-adjusted `worker_time_ms`.
* `metrics` — counts for every status, total attempts, deadline misses,
  and `resource_utilization` measured as CPU/RAM/GPU milliseconds spent
  by completed, failed, cancelled, and still-running attempts.

## 2. Complexity

Let `T` be the number of tasks (including those added by events), `W` the
number of workers, `D` the number of dependency edges, and `M` the
number of distinct *moments* visited by the discrete-event loop (bounded
above by `(end_time − start_time) / tick_ms + 1` plus the number of
events, offline edges, completion times, scripted failure times, and
rate-limit expirations).

| Operation                         | Complexity |
| --------------------------------- | ---------- |
| Cycle detection (Kahn)            | `O(T + D)` |
| Per-moment event application      | `O(E_t)` where `E_t` is events at moment `t` |
| Per-moment offline transition     | `O(W + R)` where `R` is the number of currently running tasks |
| Per-moment ready set construction | `O(T · D)` worst case (dependency lookup per pending task) |
| Per-moment scheduling             | `O(P · W)` where `P` is the number of ready tasks at the moment |
| Per-moment rate-limit check       | `O(L)` where `L` is the number of starts in the recent window |
| Heap push / pop                   | `O(log M)` per moment |
| **Total**                         | `O(M · (T · D + P · W + L + log M))` |

In practice `L` is bounded by the per-second rate limits, `P ≤ T`, and
the hot path is dominated by the simple `T · D` dependency scan. For
typical scenarios with `M` in the low thousands and `T` in the hundreds
the simulator runs in tens of milliseconds.

## 3. Edge cases handled

* Sub-`tick_ms` values fire at their exact ms: `duration_ms = 50` with
  `tick_ms = 100` finishes at 50, `fail_at_ms = 50` is logged at 50,
  `add_task` at `time_ms = 50` fires at 50, `cancel_task` at non-tick
  times cancels at the exact ms, and offline windows starting at non-tick
  times take effect at the exact ms.
* A task whose natural completion lands exactly on the moment a worker
  goes offline succeeds first (it has already finished); the offline
  transition only kills tasks that are still running at that ms.
* Dependency graphs containing cycles raise `DependencyCycleError`
  before the simulation starts. `add_task` events that would introduce a
  cycle are rejected and logged as `add_task_ignored`.
* Tasks whose `depends_on` references an unknown id are transitioned to
  `blocked` once they are pending.
* Tasks running on a worker that goes offline mid-attempt are charged a
  `failed_attempt` and become `failed` if non-retryable or out of
  retries.
* `cancel_task` for an unknown task id is logged via `cancel_ignored`.
* `cancel_task` for a task that has already terminated is logged but
  ignored.
* `cancel_task` that targets a running task releases its resources
  immediately.
* Sliding rate-limit windows are computed inclusively over the last
  1000 ms (`[t-999, t]`).
* Idempotency deduplication and conflict resolution do not consume a
  task start and therefore are not delayed by saturated start-rate
  limits.
* Worker clock skew only affects entries in the worker log; the global
  scheduling logic uses real simulation time.
* Tasks that are still running or pending when `end_time` is reached are
  reported as `pending` per the spec, and `deadline_missed` is set
  whenever `end_time > deadline_ms`.

## 4. Resolved ambiguities

The issue explicitly invites us to resolve ambiguous points. The most
important interpretations made by this implementation are:

1. **`failures[i].fail_at_ms` is relative to the start of the attempt.**
   The spec gives no anchor, but the failure scenario only makes sense if
   the failure happens *during* the attempt, so we use elapsed time
   within the attempt.
2. **Idempotency equivalence** treats two tasks as equivalent only if
   they share `duration_ms`, `cpu`, `ram`, `gpu`, `priority`,
   `deadline_ms`, `depends_on`, `retryable`, `max_retries`, and the
   exact `failures` list. Different `idempotency_key`s never compare as
   equivalent.
3. **`pending` in the output** covers both still-pending tasks and
   tasks that were running but did not finish before `end_time`.
4. **Offline windows** are interpreted as half-open intervals `[start,
   end)`. This avoids the off-by-one ambiguity where a worker might be
   reported online at the very tick that the window ends.
5. **Rate-limit windows** are sliding 1000 ms intervals. A task starting
   at time `t` counts against any window `(t', t' + 1000)` containing
   `t`. Retries also count as new starts (per the spec).
6. **Blocked detection runs every tick**, not just immediately after a
   parent fails, so tasks added later (via `add_task`) that depend on
   already-failed tasks are immediately blocked.
7. **Deadline reporting for deduplicated work** uses the deduplication
   timestamp as `finished_at`; if that timestamp is after
   `deadline_ms`, `deadline_missed` is `true`.

Each of these choices is covered by at least one unit test in
`tests/test_scheduler.py`.

## 5. Known limitations

* Resource utilization is computed as exact `cpu/ram/gpu` × elapsed-ms
  per attempt. Because time is millisecond-precise, utilization is
  millisecond-precise as well.
* The rate-limit start log grows monotonically; for very long
  simulations it could be pruned to keep memory bounded. We have not
  implemented pruning since the issue specifies bounded inputs.
* `add_task` events whose `task` payload omits required fields will
  raise during the tick; we treat input validation as the caller's
  responsibility once the simulation has started.
* No preemption: a task that has started on a worker keeps its
  resources until it finishes, fails, is cancelled, or its worker goes
  offline.
* The scheduler does not balance load across workers beyond
  "first fitting worker by id"; it deliberately optimises for
  determinism and not throughput.

## 6. Self-check (per spec §15)

### Requirements certainly satisfied

* Pure stdlib Python 3.11+ implementation.
* Deterministic priority/tiebreaking and worker selection.
* Static and dynamic dependency cycle detection.
* Resource accounting with release on completion/failure/cancellation.
* Worker offline handling with mid-flight `failed_attempt`.
* Retry/non-retryable behaviour with attempt counting.
* Sliding-window global and per-worker rate limits.
* Clock-skew-aware worker log.
* Idempotency: dedup, conflict, and same-key concurrency hold-off.
* Cancellation of pending and running tasks; ignoring cancellations of
  terminal tasks.
* `add_task` events can introduce new tasks during the simulation.
* Deadline reporting without auto-cancellation.
* Output structure matches the spec (tasks, events_log, worker_log,
  metrics).
* Resource utilization includes successful, failed, cancelled, and
  in-progress attempt time.

### Potentially debatable

* The exact semantics of `failures[i].fail_at_ms` — relative to the
  attempt versus relative to the global timeline. The accompanying
  tests fix our chosen interpretation.
* Idempotency equivalence ignores the task `id` and `idempotency_key`
  itself but compares all execution-relevant fields. Reasonable
  alternative interpretations exist.
* Whether dependencies on a `deduplicated` predecessor should treat the
  predecessor as success. We treat `deduplicated` as success-equivalent
  for downstream readiness because the keyed work is known to have
  already completed. (Tested implicitly in the issue example.)

### Most likely bugs

* Off-by-one in the rate window when `tick_ms` is large (we mitigate by
  using a strict `cutoff = t - 999`).
* Edge cases around tasks added after their declared dependencies have
  been blocked: handled by re-checking `deps_state` every tick.
* Resource utilization currently rounds duration to whole ticks; if
  sub-tick accuracy matters this should be revisited.

### Tests we would add given more time

* Hundreds-of-tasks property tests verifying that the output is a
  permutation-invariant function of the input.
* Stress tests for very tight global rate limits combined with many
  retryable failures.
* A test asserting that `cpu_time + ram_time + gpu_time` never exceeds
  the cluster capacity multiplied by simulation duration.
