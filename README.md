# fast_curl

Ultra-fast parallel HTTP client for Ruby. C extension built on libcurl `curl_multi` API.

## Features

- **Parallel requests** via `curl_multi` — no threads, no fibers needed
- **GVL release** — `rb_thread_call_without_gvl` during I/O, other Ruby threads keep running
- **Fiber scheduler compatible** — works inside `Async do ... end` without blocking other fibers
- **Three modes**: execute (all), first_execute (first N), stream_execute (yield as ready)
- **Zero dependencies** — only libcurl (available everywhere)

## Installation

**Requirements**: Ruby >= 3.1, libcurl

> **Why Ruby 3.1?** The C extension uses `rb_fiber_scheduler_current`, `rb_fiber_scheduler_block` and `rb_fiber_scheduler_unblock` to properly yield control to the Fiber Scheduler during I/O. These APIs are stable starting from Ruby 3.1. Without them, there is no correct way for a C extension to cooperate with the scheduler — earlier approaches (`rb_thread_schedule`) hold the GVL and block other fibers.

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

Benchmarks against `httpbin.org`, 5 iterations with 1 warmup, median times.
Run yourself: `bundle exec ruby benchmark/local_bench.rb`.

Each request hits `/delay/1` (server-side 1-second delay), so sequential baseline
grows linearly while parallel clients stay near ~1s plus network overhead.

### Time to completion (lower is better)

| Scenario                        | Net::HTTP sequential | fast_curl (thread) | fast_curl (fiber/Async) | Async::HTTP::Internet |
|---------------------------------|---------------------:|-------------------:|------------------------:|----------------------:|
| 4 requests × 1s, conn=4         |                8.27s |              2.36s |                   2.13s |                 2.56s |
| 10 requests × 1s, conn=10       |               20.92s |              3.49s |                   5.23s |                 3.83s |
| 20 requests × 1s, conn=5        |               42.56s |              2.94s |                   2.90s |                12.14s |
| 200 requests × 1s, conn=20      |                   —  |             22.19s |                  21.77s |                23.59s |

### Speedup vs Net::HTTP (median)

| Scenario                          | fast_curl (thread) | fast_curl (fiber) | Async::HTTP |
|-----------------------------------|-------------------:|------------------:|------------:|
| 4 requests × 1s                   |           **3.5x** |              3.9x |        3.2x |
| 10 requests × 1s                  |           **6.0x** |              4.0x |        5.5x |
| 20 requests × 1s (queued, conn=5) |          **14.5x** |             14.7x |        3.5x |

### Memory & allocations per request batch (lower is better)

| Scenario                        | fast_curl (thread) allocated | fast_curl (fiber) allocated | Async::HTTP allocated |
|---------------------------------|-----------------------------:|----------------------------:|----------------------:|
| 4 requests × 1s                 |                 **278 obj**  |                     350 obj |             2,433 obj |
| 10 requests × 1s                |                 **490 obj**  |                     756 obj |             4,763 obj |
| 20 requests × 1s, conn=5        |                 **621 obj**  |                     750 obj |             8,536 obj |
| 200 requests × 1s, conn=20      |               **5,188 obj**  |                   5,642 obj |            78,203 obj |

Ruby heap delta stays near zero across all scenarios for fast_curl — most allocation
happens in C, not on the Ruby heap.

### Error handling

| Scenario                                                     |  Time |
|--------------------------------------------------------------|------:|
| 4 mixed requests (404, 500, DNS fail, 30s delay), timeout=2s | 4.02s |

Bounded by `timeout=2s` rather than by the slow request.

### Notes on the numbers

- **Net::HTTP sequential** is the proof-of-parallelism baseline — it confirms fast_curl and Async are actually running concurrently, not that they "beat" a different library. 4×1s sequentially = 4s, parallel = ~1s + overhead.
- **Variance is high against remote endpoints** (httpbin.org). For stable numbers, use `--local` which spawns a WEBrick server on 127.0.0.1.
- **fast_curl (thread) vs (fiber)**: same underlying C code, different scheduling. "thread" is the default; "fiber" kicks in automatically when called inside `Async do ... end`.

## License

MIT
