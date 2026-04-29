### Added

- Deterministic distributed task scheduler simulator: `run_simulation`
  in `my_package` plus public exceptions `SchedulerError` and
  `DependencyCycleError`.
- Architecture and edge-case documentation in `docs/SCHEDULER.md`.
- 22 unit tests covering basic execution, dependencies, retries, rate
  limits, resources, offline workers, idempotency, events, deadlines,
  priority ordering, clock skew, and the issue's example input.

### Changed

- Bumped package version from `0.1.0` to `0.2.0`.
