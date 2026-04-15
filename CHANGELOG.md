# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-04-15

### Changed
- **Minimum Ruby version raised to 3.1** — required for `rb_fiber_scheduler_current`, `rb_fiber_scheduler_block`/`unblock` APIs used in the new Fiber Scheduler integration

### Fixed
- **Fiber Scheduler: proper GVL release** — replaced `rb_thread_schedule()` busy-loop with `rb_thread_create` + `rb_thread_call_without_gvl` + `rb_fiber_scheduler_block`/`unblock` pattern, so the fiber scheduler can actually run other fibers during I/O
- **Fiber Scheduler: retry_delay_sleep blocked GVL** — `nanosleep` was called directly without releasing GVL in fiber path; now uses the same fiber worker pattern
- **GVL: monolithic perform loop** — batch execute previously ran the entire `curl_multi_poll` loop in a single `rb_thread_call_without_gvl` call, blocking GC and thread interrupts for the full duration; now uses iterative single-poll-per-GVL-release for all modes
- **Thread interruption (Thread#kill)** — `unblock_perform` was a no-op; now sets cancellation flag and calls `curl_multi_wakeup` to actually interrupt `curl_multi_poll`
- **Memory leak on exceptions** — `rb_yield` in `stream_execute` or any Ruby API call could raise, leaking all curl resources; wrapped in `rb_ensure` for guaranteed cleanup
- **Header injection** — header keys and values containing `\r`, `\n`, or `\0` are now silently rejected
- **Request count overflow** — `RARRAY_LEN` (long) → int cast is now bounds-checked; batch size limited to 10,000

### Added
- `RB_GC_GUARD` for Ruby string objects passed to curl handles
- GVL/Fiber scheduler test suite (`test_gvl_and_fiber_scheduler.rb`) verifying that other threads and fibers actually run during I/O

## [0.2.0] - 2024-03-25

### Added
- **Retry functionality**: Automatic retry for failed requests
  - Configurable retry attempts via `retries` option (default: 1, max: 10)
  - Configurable retry delay via `retry_delay` option (in milliseconds)
  - Custom HTTP status codes for retry via `retry_codes` option
  - Automatic retry for network errors (timeout, connection issues, etc.)
  - Smart retry logic that avoids retrying DNS resolution errors
- Proper warnings when retry is not supported (stream_execute, first_execute)

### Technical Details
- Retry logic implemented in C extension for optimal performance
- Default retryable CURL error codes: timeout, connection errors, send/receive errors
- Retry delay implemented with proper GVL release and fiber scheduler support
- All retry attempts respect the original request timeout settings

## [0.1.1] - 2024-03-24

### Added
- HTTP/2 multiplexing support

## [0.1.0] - 2024-03-24

### Added
- Initial release
- Parallel HTTP requests via libcurl multi API
- GVL release during I/O operations
- Fiber scheduler compatibility
- Multiple execution modes (execute, first_execute, stream_execute)
- Support for GET, POST, PUT, DELETE, PATCH methods
- Zero dependencies (only libcurl required)
