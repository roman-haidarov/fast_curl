# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-22

### Fixed

- **Non-idempotent requests are no longer replayed (security/data integrity).**
  `retries` defaulted to `1` and applied to every method, so a `POST` that failed
  with `CURLE_GOT_NOTHING`/`SEND_ERROR`/`RECV_ERROR`/`PARTIAL_FILE` — errors that
  routinely occur *after* the server has accepted and processed the request — was
  silently sent a second time. The caller saw an error while the server saw two
  charges. Only idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS) are retried
  now; opt in with `retry_non_idempotent: true`.
- **No wall-clock budget across retries.** `timeout` is per attempt, so
  `timeout: 30, retries: 10` could run for 330s. Added `total_timeout` (ms),
  which bounds the entire call and clamps each attempt's timeout to the time left.
- **Thread churn under a Fiber scheduler.** `stream_execute` created a new OS
  thread for every 50ms poll (~20/second, ~600 for a 30s stream). The poll loop
  now runs until a transfer completes or a 2s slice expires; cancellation still
  interrupts immediately via `curl_multi_wakeup`.
- **Repeated DNS lookups and TLS handshakes between calls.** Added a
  process-wide `CURLSH` sharing the DNS cache and TLS session cache, with
  pthread locking. TCP connections themselves are still *not* pooled across
  calls — see Known limitations.
- **One bad request destroyed the whole batch.** An invalid header name, bad URL
  scheme or unsupported method raised `ArgumentError` from the middle of the run
  loop, discarding every other response. These are now per-request soft failures.
- **Header name case depended on the negotiated protocol.** Names were returned
  verbatim, so HTTP/2 (always lowercase) and HTTP/1.1 produced different keys for
  the same code. Names are normalised to lowercase and `FastCurl::Headers` looks
  up case-insensitively.
- **Multi-value headers had an unstable type.** One `Set-Cookie` returned a
  String, two returned an Array. `set-cookie` is now always an Array; other
  repeated fields fold into one comma-separated String (RFC 9110 5.3).
- **String bodies were mislabelled.** libcurl defaults a raw `POSTFIELDS` body to
  `application/x-www-form-urlencoded`, so a hand-built JSON string went out
  labelled as a form. Use `json:`/`form:` to be explicit; a raw `body:` String now
  defaults to `application/octet-stream`.

### Added

- `total_timeout`, `connect_timeout`, `follow_redirects`, `max_redirects` and
  `retry_non_idempotent` options. There was previously no connect timeout at all
  and redirects could not be disabled.
- Exponential backoff with full jitter between retries; `retry_delay` now
  defaults to 100ms instead of 0 and is the base of the backoff.
- `:error` (Symbol), `:effective_url` and `:attempts` on every response. Failed
  responses now carry exactly the same keys as successful ones.
- `json:`, `form:` and `params:` request options.
- `FastCurl::Headers`, a case-insensitive Hash subclass.

- **`stream_execute` invoked the caller's block with garbage.** The response
  headers hash was created with `rb_class_new_instance`, which runs
  `Hash#initialize` and passes it the block of the current frame. Inside
  `stream_execute` the user's block therefore became the hash's default proc and
  was invoked with `(hash, key)` on every first-seen header, before the real
  `[index, response]` pair. Found by running under the real `async` gem.

### Known limitations

- **TCP connections are not reused between separate calls.** Each call builds
  its own multi handle. Sharing libcurl's connection cache
  (`CURL_LOCK_DATA_CONNECT`) was implemented and then reverted: with several
  threads running batches concurrently it deadlocks against
  `CURLMOPT_MAX_TOTAL_CONNECTIONS`, and without that limit libcurl 8.5.0
  segfaults inside `curl_multi_perform`. A correct fix needs a persistent
  per-thread multi handle. Regression test:
  `TestConcurrency#test_batches_from_many_threads`.
- On Ruby 2.7 and 3.0 the Fiber Scheduler code is compiled out, so a request
  made inside a scheduler blocks the whole thread and no sibling fiber runs
  until it completes. Verified by building with the scheduler C API disabled:
  the functional suite passes, the starvation tests fail as expected.
- Cancellation of an in-flight request relies on `curl_multi_wakeup`; on libcurl
  older than 7.68 it is a no-op and a killed thread or stopped task waits for the
  request to finish.

### Changed

- **Breaking:** response header keys are lowercase; `set-cookie` is always an
  Array; a raw String `body:` is labelled `application/octet-stream`; `POST` and
  `PATCH` are not retried unless `retry_non_idempotent: true`.

## [0.3.1] - 2026-04-24

### Added
- **Ruby 2.7 support** — minimum supported Ruby version lowered to `>= 2.7.0`; Fiber Scheduler-specific code is now guarded so the extension can build and run on Ruby versions without those C APIs.

### Fixed
- **Async/Fiber starvation** — long-running `FastCurl.get`/`execute` calls no longer hold the Async task and starve sibling fibers; the fiber path now delegates the blocking curl execution to a worker thread for the duration of the call so the scheduler can continue running other tasks.
- **Async regression coverage** — added ticker/starvation tests to verify that another Async task keeps making progress while FastCurl performs long I/O.

### Changed
- **C extension refactoring** — reduced repetition and boilerplate with table-driven method dispatch, shared option parsing helpers, consolidated header formatting, and simpler cleanup paths without moving runtime logic into Ruby.

## [0.3.0] - 2026-04-15

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

## [0.2.0] - 2026-03-25

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

## [0.1.1] - 2026-03-24

### Added
- HTTP/2 multiplexing support

## [0.1.0] - 2026-03-21

### Added
- Initial release
- Parallel HTTP requests via libcurl multi API
- GVL release during I/O operations
- Fiber scheduler compatibility
- Multiple execution modes (execute, first_execute, stream_execute)
- Support for GET, POST, PUT, DELETE, PATCH methods
- Zero dependencies (only libcurl required)
