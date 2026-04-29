"""Unit tests for the deterministic task scheduler simulator."""

from __future__ import annotations

import copy
import unittest
from typing import Any

from my_package import DependencyCycleError, run_simulation


def make_input(
    *,
    tasks: list[dict[str, Any]] | None = None,
    workers: list[dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
    end_time: int = 30000,
    tick_ms: int = 100,
    global_rate: int = 100,
) -> dict[str, Any]:
    """Build a minimal input document with sensible defaults."""
    return {
        "simulation": {
            "start_time": 0,
            "end_time": end_time,
            "tick_ms": tick_ms,
            "global_rate_limit_per_sec": global_rate,
        },
        "workers": workers
        if workers is not None
        else [
            {
                "id": "w1",
                "cpu": 8,
                "ram": 16000,
                "gpu": 1,
                "local_rate_limit_per_sec": 100,
                "clock_skew_ms": 0,
                "offline_windows": [],
            }
        ],
        "tasks": tasks or [],
        "events": events or [],
    }


def task(
    tid: str,
    *,
    duration_ms: int = 1000,
    cpu: int = 1,
    ram: int = 100,
    gpu: int = 0,
    priority: int = 5,
    deadline_ms: int = 100000,
    depends_on: list[str] | None = None,
    retryable: bool = False,
    max_retries: int = 0,
    failures: list[dict[str, int]] | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    return {
        "id": tid,
        "duration_ms": duration_ms,
        "cpu": cpu,
        "ram": ram,
        "gpu": gpu,
        "priority": priority,
        "deadline_ms": deadline_ms,
        "depends_on": depends_on or [],
        "retryable": retryable,
        "max_retries": max_retries,
        "failures": failures or [],
        "idempotency_key": idempotency_key,
    }


class TestBasicExecution(unittest.TestCase):
    """Smoke tests for happy paths."""

    def test_single_task_succeeds(self) -> None:
        out = run_simulation(make_input(tasks=[task("A", duration_ms=500)]))
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertEqual(out["tasks"]["A"]["attempts"], 1)
        self.assertEqual(out["tasks"]["A"]["started_at"], 0)
        self.assertEqual(out["tasks"]["A"]["finished_at"], 500)
        self.assertFalse(out["tasks"]["A"]["deadline_missed"])
        self.assertEqual(out["metrics"]["success"], 1)
        self.assertEqual(out["metrics"]["total_attempts"], 1)

    def test_deterministic_output(self) -> None:
        """Same input should produce identical output, byte for byte."""
        inp = make_input(
            tasks=[
                task("B", priority=5),
                task("A", priority=5),
                task("C", priority=5),
            ]
        )
        a = run_simulation(copy.deepcopy(inp))
        b = run_simulation(copy.deepcopy(inp))
        self.assertEqual(a, b)


class TestDependencies(unittest.TestCase):
    def test_dependency_blocks_until_parent_succeeds(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task("A", duration_ms=1000),
                    task("B", duration_ms=500, depends_on=["A"]),
                ]
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertEqual(out["tasks"]["B"]["status"], "success")
        self.assertGreaterEqual(
            out["tasks"]["B"]["started_at"], out["tasks"]["A"]["finished_at"]
        )

    def test_dependent_blocked_when_parent_fails(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "A",
                        duration_ms=2000,
                        retryable=False,
                        failures=[{"attempt": 1, "fail_at_ms": 500}],
                    ),
                    task("B", depends_on=["A"]),
                ]
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "failed")
        self.assertEqual(out["tasks"]["B"]["status"], "blocked")

    def test_dependency_cycle_raises(self) -> None:
        with self.assertRaises(DependencyCycleError):
            run_simulation(
                make_input(
                    tasks=[
                        task("A", depends_on=["B"]),
                        task("B", depends_on=["A"]),
                    ]
                )
            )


class TestRetry(unittest.TestCase):
    def test_retryable_task_recovers(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "A",
                        duration_ms=1000,
                        retryable=True,
                        max_retries=2,
                        failures=[{"attempt": 1, "fail_at_ms": 200}],
                    )
                ]
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertEqual(out["tasks"]["A"]["attempts"], 2)
        self.assertEqual(out["metrics"]["total_attempts"], 2)

    def test_non_retryable_fails_after_first_error(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "A",
                        duration_ms=1000,
                        retryable=False,
                        failures=[{"attempt": 1, "fail_at_ms": 100}],
                    )
                ]
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "failed")
        self.assertEqual(out["tasks"]["A"]["attempts"], 1)
        self.assertEqual(out["tasks"]["A"]["failure_reason"], "scripted_failure")

    def test_retry_exhausted(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "A",
                        duration_ms=1000,
                        retryable=True,
                        max_retries=1,
                        failures=[
                            {"attempt": 1, "fail_at_ms": 100},
                            {"attempt": 2, "fail_at_ms": 100},
                        ],
                    )
                ]
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "failed")
        self.assertEqual(out["tasks"]["A"]["attempts"], 2)


class TestRateLimits(unittest.TestCase):
    def test_global_rate_limit_throttles_starts(self) -> None:
        # 5 tasks, global rate of 2/sec, each 100 ms → only 2 can start in
        # the first second.
        tasks = [task(f"T{i}", duration_ms=100) for i in range(5)]
        out = run_simulation(
            make_input(
                tasks=tasks,
                global_rate=2,
                end_time=10000,
                workers=[
                    {
                        "id": "w1",
                        "cpu": 100,
                        "ram": 1000000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 100,
                        "clock_skew_ms": 0,
                        "offline_windows": [],
                    }
                ],
            )
        )
        starts = sorted(out["tasks"][f"T{i}"]["started_at"] for i in range(5))
        # The first two start at 0, the next two at >= 1000, the fifth at
        # >= 1000 as well (since window is 1 second).
        self.assertEqual(starts[0], 0)
        self.assertEqual(starts[1], 0)
        self.assertGreaterEqual(starts[2], 1000)

    def test_local_rate_limit_throttles_per_worker(self) -> None:
        tasks = [task(f"T{i}", duration_ms=100) for i in range(3)]
        out = run_simulation(
            make_input(
                tasks=tasks,
                global_rate=100,
                end_time=10000,
                workers=[
                    {
                        "id": "w1",
                        "cpu": 100,
                        "ram": 1000000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 1,
                        "clock_skew_ms": 0,
                        "offline_windows": [],
                    }
                ],
            )
        )
        starts = sorted(out["tasks"][f"T{i}"]["started_at"] for i in range(3))
        # Only one start per second on this single worker.
        self.assertEqual(starts[0], 0)
        self.assertGreaterEqual(starts[1], 1000)
        self.assertGreaterEqual(starts[2], 2000)


class TestResources(unittest.TestCase):
    def test_resource_starvation_serializes_tasks(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task("A", duration_ms=1000, cpu=4, ram=4000),
                    task("B", duration_ms=1000, cpu=4, ram=4000),
                    task("C", duration_ms=1000, cpu=4, ram=4000),
                ],
                workers=[
                    {
                        "id": "w1",
                        "cpu": 4,
                        "ram": 4000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 100,
                        "clock_skew_ms": 0,
                        "offline_windows": [],
                    }
                ],
            )
        )
        # Only one fits at a time on the single worker.
        for tid in ("A", "B", "C"):
            self.assertEqual(out["tasks"][tid]["status"], "success")
        starts = sorted(out["tasks"][t]["started_at"] for t in ("A", "B", "C"))
        self.assertEqual(starts[0], 0)
        self.assertGreaterEqual(starts[1], 1000)
        self.assertGreaterEqual(starts[2], 2000)


class TestOfflineWorker(unittest.TestCase):
    def test_offline_worker_kills_running_task(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=5000, retryable=False)],
                workers=[
                    {
                        "id": "w1",
                        "cpu": 8,
                        "ram": 16000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 100,
                        "clock_skew_ms": 0,
                        "offline_windows": [[2000, 4000]],
                    }
                ],
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "failed")
        self.assertEqual(out["tasks"]["A"]["failure_reason"], "worker_offline")


class TestIdempotency(unittest.TestCase):
    def test_duplicate_key_results_in_dedup(self) -> None:
        # B is added by an event after A succeeds so the two tasks are not
        # ready simultaneously; they have identical parameters so B should
        # be deduplicated.
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=500, idempotency_key="K")],
                end_time=5000,
                events=[
                    {
                        "time_ms": 1000,
                        "type": "add_task",
                        "task": task("B", duration_ms=500, idempotency_key="K"),
                    }
                ],
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertEqual(out["tasks"]["B"]["status"], "deduplicated")

    def test_conflicting_params_yield_idempotency_conflict(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=500, idempotency_key="K")],
                end_time=5000,
                events=[
                    {
                        "time_ms": 1000,
                        "type": "add_task",
                        "task": task(
                            "B",
                            duration_ms=999,  # different params!
                            idempotency_key="K",
                        ),
                    }
                ],
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertEqual(out["tasks"]["B"]["status"], "idempotency_conflict")


class TestEvents(unittest.TestCase):
    def test_cancel_running_task_releases_resources(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=5000)],
                events=[
                    {"time_ms": 1000, "type": "cancel_task", "task_id": "A"},
                ],
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "cancelled")
        self.assertEqual(out["tasks"]["A"]["finished_at"], 1000)

    def test_cancel_completed_task_is_logged_but_ignored(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=500)],
                events=[
                    {"time_ms": 2000, "type": "cancel_task", "task_id": "A"},
                ],
            )
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        types = [e["type"] for e in out["events_log"]]
        self.assertIn("cancel_ignored", types)

    def test_add_task_event_introduces_task(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[],
                end_time=5000,
                events=[
                    {
                        "time_ms": 1000,
                        "type": "add_task",
                        "task": task("Z", duration_ms=500),
                    }
                ],
            )
        )
        self.assertIn("Z", out["tasks"])
        self.assertEqual(out["tasks"]["Z"]["status"], "success")
        # Z cannot start before being added.
        self.assertGreaterEqual(out["tasks"]["Z"]["started_at"], 1000)


class TestDeadlines(unittest.TestCase):
    def test_deadline_missed_after_completion(self) -> None:
        out = run_simulation(
            make_input(tasks=[task("A", duration_ms=2000, deadline_ms=1000)])
        )
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        self.assertTrue(out["tasks"]["A"]["deadline_missed"])
        self.assertEqual(out["metrics"]["deadline_missed"], 1)

    def test_blocked_task_with_deadline(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "A",
                        duration_ms=1000,
                        retryable=False,
                        failures=[{"attempt": 1, "fail_at_ms": 100}],
                    ),
                    task("B", depends_on=["A"], deadline_ms=500),
                ]
            )
        )
        self.assertEqual(out["tasks"]["B"]["status"], "blocked")
        self.assertTrue(out["tasks"]["B"]["deadline_missed"])


class TestPriorityOrdering(unittest.TestCase):
    def test_priority_then_deadline_then_id(self) -> None:
        # Single CPU forces serial execution; we observe ordering.
        out = run_simulation(
            make_input(
                tasks=[
                    task(
                        "C",
                        duration_ms=500,
                        cpu=1,
                        priority=1,
                        deadline_ms=10000,
                    ),
                    task(
                        "B",
                        duration_ms=500,
                        cpu=1,
                        priority=5,
                        deadline_ms=5000,
                    ),
                    task(
                        "A",
                        duration_ms=500,
                        cpu=1,
                        priority=5,
                        deadline_ms=2000,
                    ),
                ],
                workers=[
                    {
                        "id": "w1",
                        "cpu": 1,
                        "ram": 1000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 100,
                        "clock_skew_ms": 0,
                        "offline_windows": [],
                    }
                ],
            )
        )
        a_start = out["tasks"]["A"]["started_at"]
        b_start = out["tasks"]["B"]["started_at"]
        c_start = out["tasks"]["C"]["started_at"]
        # A has priority 5, deadline 2000 → first.
        # B has priority 5, deadline 5000 → second.
        # C has priority 1 → last.
        self.assertLess(a_start, b_start)
        self.assertLess(b_start, c_start)


class TestClockSkew(unittest.TestCase):
    def test_clock_skew_only_affects_worker_log(self) -> None:
        out = run_simulation(
            make_input(
                tasks=[task("A", duration_ms=500)],
                workers=[
                    {
                        "id": "w1",
                        "cpu": 8,
                        "ram": 1000,
                        "gpu": 0,
                        "local_rate_limit_per_sec": 100,
                        "clock_skew_ms": 250,
                        "offline_windows": [],
                    }
                ],
            )
        )
        # Logic uses global time → started_at == 0.
        self.assertEqual(out["tasks"]["A"]["started_at"], 0)
        # Worker log records the skewed timestamp.
        first = out["worker_log"]["w1"][0]
        self.assertEqual(first["global_time_ms"], 0)
        self.assertEqual(first["worker_time_ms"], 250)


class TestExampleFromIssue(unittest.TestCase):
    """End-to-end validation using the exact JSON from the issue."""

    INPUT: dict[str, Any] = {
        "simulation": {
            "start_time": 0,
            "end_time": 30000,
            "tick_ms": 100,
            "global_rate_limit_per_sec": 5,
        },
        "workers": [
            {
                "id": "w1",
                "cpu": 8,
                "ram": 16000,
                "gpu": 1,
                "local_rate_limit_per_sec": 3,
                "clock_skew_ms": 50,
                "offline_windows": [[7000, 10000]],
            },
            {
                "id": "w2",
                "cpu": 4,
                "ram": 8000,
                "gpu": 0,
                "local_rate_limit_per_sec": 2,
                "clock_skew_ms": -100,
                "offline_windows": [],
            },
        ],
        "tasks": [
            {
                "id": "A",
                "duration_ms": 3000,
                "cpu": 2,
                "ram": 2000,
                "gpu": 0,
                "priority": 5,
                "deadline_ms": 10000,
                "depends_on": [],
                "retryable": True,
                "max_retries": 2,
                "failures": [],
                "idempotency_key": "alpha",
            },
            {
                "id": "B",
                "duration_ms": 4000,
                "cpu": 4,
                "ram": 4000,
                "gpu": 1,
                "priority": 9,
                "deadline_ms": 12000,
                "depends_on": ["A"],
                "retryable": True,
                "max_retries": 1,
                "failures": [{"attempt": 1, "fail_at_ms": 2000}],
                "idempotency_key": "beta",
            },
        ],
        "events": [
            {"time_ms": 5000, "type": "cancel_task", "task_id": "X"},
            {
                "time_ms": 9000,
                "type": "add_task",
                "task": {
                    "id": "C",
                    "duration_ms": 2000,
                    "cpu": 1,
                    "ram": 1000,
                    "gpu": 0,
                    "priority": 10,
                    "deadline_ms": 14000,
                    "depends_on": ["B"],
                    "retryable": False,
                    "max_retries": 0,
                    "failures": [],
                    "idempotency_key": "gamma",
                },
            },
        ],
    }

    def test_issue_example_runs(self) -> None:
        out = run_simulation(copy.deepcopy(self.INPUT))
        self.assertIn("A", out["tasks"])
        self.assertIn("B", out["tasks"])
        self.assertIn("C", out["tasks"])
        self.assertEqual(out["tasks"]["A"]["status"], "success")
        # Required output sections exist.
        self.assertIn("events_log", out)
        self.assertIn("worker_log", out)
        self.assertIn("metrics", out)
        for key in (
            "success",
            "failed",
            "cancelled",
            "blocked",
            "deduplicated",
            "idempotency_conflict",
            "pending",
            "deadline_missed",
            "total_attempts",
            "resource_utilization",
        ):
            self.assertIn(key, out["metrics"])


if __name__ == "__main__":
    unittest.main()
