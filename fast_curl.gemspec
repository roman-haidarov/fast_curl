# frozen_string_literal: true

require_relative "lib/fast_curl/version"

Gem::Specification.new do |spec|
  spec.name          = "fast_curl"
  spec.version       = FastCurl::VERSION
  spec.authors       = ["roman-haidarov"]
  spec.email         = ["roman.haidarov@gmail.com"]

  spec.summary       = "Ultra-fast parallel HTTP client as Ruby C extension on libcurl multi"
  spec.description   = "Parallel HTTP requests via libcurl curl_multi API. " \
                        "Releases GVL during I/O, compatible with Async gem and Fiber scheduler. " \
                        "Supports execute (all), first_execute (first N), stream_execute (yield as ready). " \
                        "Built-in retry functionality for network errors and custom HTTP status codes."
  spec.homepage      = "https://github.com/roman-haidarov/fast_curl"
  spec.license       = "MIT"

  spec.required_ruby_version = ">= 3.0.0"

  spec.files         = Dir["lib/**/*.rb", "ext/**/*.{c,h,rb}", "LICENSE.txt", "README.md"]
  spec.extensions    = ["ext/fast_curl/extconf.rb"]
  spec.require_paths = ["lib"]

  # Runtime dependencies
  spec.add_dependency "json", "~> 2.0"

  # Development dependencies  
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler", "~> 1.2"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "benchmark-ips", "~> 2.0"
  spec.add_development_dependency "webrick", "~> 1.8"
end
