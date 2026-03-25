# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
