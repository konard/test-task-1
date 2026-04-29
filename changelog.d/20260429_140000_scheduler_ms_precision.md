### Changed

- Scheduler simulator now uses a discrete-event time advance with
  millisecond precision instead of snapping every action to a
  `tick_ms` boundary.
  - `duration_ms` smaller than `tick_ms` finishes at the exact ms
    (e.g. `duration_ms = 50` with `tick_ms = 100` finishes at 50).
  - `failures[i].fail_at_ms` triggers and is logged at the exact ms.
  - `add_task` and `cancel_task` events fire at their exact `time_ms`,
    even when that ms is not aligned to `tick_ms`.
  - Offline-window edges take effect at the exact ms.
  - A task whose natural completion lands on the same ms a worker goes
    offline succeeds first; the offline transition only kills tasks
    that were still running at that ms.
  - `tick_ms` is now a periodic re-evaluation cadence only.

### Added

- Seven additional unit tests covering millisecond-precision time
  semantics: sub-tick durations, sub-tick scripted failures, off-tick
  events, off-tick offline windows, the offline-vs-finish edge case,
  and tick-independence of task outcomes.
