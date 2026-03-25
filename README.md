# fast_curl

Ultra-fast parallel HTTP client for Ruby. C extension built on libcurl `curl_multi` API.

## Features

- **Parallel requests** via `curl_multi` — no threads, no fibers needed
- **GVL release** — `rb_thread_call_without_gvl` during I/O, other Ruby threads keep running
- **Fiber scheduler compatible** — works inside `Async do ... end` without blocking other fibers
- **Three modes**: execute (all), first_execute (first N), stream_execute (yield as ready)
- **Zero dependencies** — only libcurl (available everywhere)

## Installation

**Requirements**: Ruby >= 3.0 (for Fiber scheduler support)

```ruby
gem 'fast_curl'
```

Requires libcurl development headers:

```bash
# macOS
brew install curl

# Ubuntu/Debian
apt-get install libcurl4-openssl-dev

# Alpine
apk add curl-dev
```

## Usage

### Basic GET

```ruby
results = FastCurl.get([
  { url: "https://api.example.com/users" },
  { url: "https://api.example.com/posts" }
], connections: 20, timeout: 30)

results.each do |index, response|
  puts "#{index}: #{response[:status]} — #{response[:body]}"
end
```

### POST with body and headers

```ruby
FastCurl.post([
  {
    url: "https://api.example.com/users",
    headers: { "Authorization" => "Bearer token" },
    body: { name: "John" }
  }
])
```

### First N responses (cancel the rest)

```ruby
result = FastCurl.first_get([
  { url: "https://mirror1.example.com/file" },
  { url: "https://mirror2.example.com/file" },
  { url: "https://mirror3.example.com/file" }
], count: 1)
```

### Stream responses as they arrive

```ruby
FastCurl.stream_get(urls, connections: 50) do |index, response|
  puts "Got response #{index}: #{response[:status]}"
end
```

### Retry functionality (v0.2.0+)

```ruby
# Automatic retry on network errors (timeout, connection issues)
results = FastCurl.get([
  { url: "https://unreliable-api.com/data" }
], retries: 3, retry_delay: 1000)  # 3 retries with 1s delay

# Retry on specific HTTP status codes
results = FastCurl.get([
  { url: "https://api.example.com/data" }
], retries: 2, retry_codes: [500, 502, 503], retry_delay: 500)

# Disable retries (default is 1 retry)
results = FastCurl.get(urls, retries: 0)
```

### Inside Async

```ruby
require "async"

Async do
  # fast_curl detects the fiber scheduler and yields
  # to other fibers during I/O instead of blocking
  results = FastCurl.get(urls, connections: 20)
end
```

## Response format

```ruby
[index, {
  status: 200,             # HTTP status code (0 on error)
  headers: { "Key" => "Value" },
  body: "response body"
}]
```

## Available methods

| Method | Description |
|---|---|
| `FastCurl.get(requests, **opts)` | GET all, wait for all |
| `FastCurl.post(requests, **opts)` | POST all, wait for all |
| `FastCurl.put(requests, **opts)` | PUT all, wait for all |
| `FastCurl.delete(requests, **opts)` | DELETE all, wait for all |
| `FastCurl.patch(requests, **opts)` | PATCH all, wait for all |
| `FastCurl.first_get(requests, count: 1, **opts)` | GET, return first N |
| `FastCurl.stream_get(requests, **opts) { \|i, r\| }` | GET, yield each |
| `FastCurl.execute(requests, **opts)` | Raw execute |
| `FastCurl.first_execute(requests, count: 1, **opts)` | Raw first N |
| `FastCurl.stream_execute(requests, **opts) { \|pair\| }` | Raw stream |

## Options

| Option | Default | Description |
|---|---|---|
| `connections` | 20 | Max parallel connections |
| `timeout` | 30 | Per-request timeout in seconds |
| `retries` | 1 | Number of retry attempts (0-10) |
| `retry_delay` | 0 | Delay between retries in milliseconds |
| `retry_codes` | [] | HTTP status codes to retry on |

## Performance

Benchmark results (`bundle exec ruby benchmark/local_bench.rb`):

| Method | 4 parallel | 10 parallel | 20 parallel | 200 parallel |
|--------|------------|-------------|--------------|---------------|
| Net::HTTP sequential | 7.93s (+2.1 MB) | 24.20s (+0.3 MB) | 48.58s (+1.2 MB) | - |
| fast_curl (thread) | 2.09s (+0.7 MB) | 3.73s (+0.9 MB) | 3.76s (+0.0 MB) | 5.88s (+2.3 MB) |
| fast_curl (fiber) | 1.96s (+0.4 MB) | 4.86s (+0.0 MB) | 3.71s (+0.2 MB) | 9.60s (+1.6 MB) |
| Async::HTTP | 2.54s (+0.3 MB) | 4.27s (+0.4 MB) | 9.16s (+0.5 MB) | 22.44s (+10.7 MB) |

Additional scenarios:
- Stream execute (5 requests): 5.99s (+0.0 MB)
- First execute (first 1 of 5): 2.40s (+0.0 MB)
- Error handling (timeout=2s): 2.01s (+0.0 MB)

## License

MIT
