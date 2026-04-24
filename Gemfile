# frozen_string_literal: true

source "https://rubygems.org"

gemspec

ruby_version = Gem::Version.new(RUBY_VERSION)

group :development, :test do
  gem "rake", ">= 13.0", "< 14"
  gem "rake-compiler", ">= 1.2", "< 2"
  gem "minitest", ">= 5.14", "< 6"
  gem "webrick", ">= 1.8", "< 2"
end

group :benchmark, optional: true do
  gem "benchmark-ips", ">= 2.0", "< 3"

  if ruby_version >= Gem::Version.new("3.0")
    gem "async", ">= 2.0", "< 3"
    gem "async-http", ">= 0.60", "< 1"
  else
    gem "async", ">= 1.25", "< 2"
    gem "async-http", ">= 0.56", "< 0.60"
  end
end
