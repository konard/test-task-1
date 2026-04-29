"""Quick smoke test using the example input from the issue."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from my_package import run_simulation  # noqa: E402


EXAMPLE_INPUT = {
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


def main() -> None:
    result = run_simulation(EXAMPLE_INPUT)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
