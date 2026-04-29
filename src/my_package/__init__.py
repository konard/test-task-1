"""Public package entry point.

Exposes both the legacy template helpers (``add``, ``multiply``, ``delay``)
and the deterministic distributed task scheduler simulator
(:func:`run_simulation`).
"""

from __future__ import annotations

from my_package.scheduler import (
    DependencyCycleError,
    SchedulerError,
    run_simulation,
)

__version__ = "0.2.0"

__all__ = [
    "DependencyCycleError",
    "SchedulerError",
    "add",
    "delay",
    "multiply",
    "run_simulation",
]


def add(a: int | float, b: int | float) -> int | float:
    """Add two numbers.

    Args:
        a: First number
        b: Second number

    Returns:
        Sum of a and b
    """
    return a + b


def multiply(a: int | float, b: int | float) -> int | float:
    """Multiply two numbers.

    Args:
        a: First number
        b: Second number

    Returns:
        Product of a and b
    """
    return a * b


async def delay(seconds: float) -> None:
    """Async delay function.

    Args:
        seconds: Seconds to wait
    """
    import asyncio

    await asyncio.sleep(seconds)
