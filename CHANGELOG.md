# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
