# frozen_string_literal: true

require 'json'
require_relative "fast_curl/version"
require_relative "fast_curl/fast_curl"

module FastCurl
  class Error < StandardError; end
  class TimeoutError < Error; end

  DEFAULT_OPTIONS = {
    connections: 20,
    timeout: 30
  }.freeze

  METHODS = %i[get post put delete patch].freeze
  BODY_METHODS = %i[post put patch].freeze

  class << self
    METHODS.each do |method|
      define_method(method) do |requests, **options|
        execute(build_requests(requests, method), **DEFAULT_OPTIONS.merge(options))
      end

      define_method(:"first_#{method}") do |requests, count: 1, **options|
        first_execute(build_requests(requests, method), count: count, **DEFAULT_OPTIONS.merge(options))
      end

      define_method(:"stream_#{method}") do |requests, **options, &block|
        stream_execute(build_requests(requests, method), **DEFAULT_OPTIONS.merge(options), &block)
      end
    end

    private

    def build_requests(requests, method)
      requests.map do |req|
        r = { url: req[:url], method: method.to_s.upcase }
        r[:headers] = req[:headers] if req[:headers]
        if req[:body] && BODY_METHODS.include?(method)
          r[:body] = req[:body].is_a?(Hash) ? req[:body].to_json : req[:body].to_s
          r[:headers] = (r[:headers] || {}).merge("Content-Type" => "application/json") if req[:body].is_a?(Hash)
        end
        r
      end
    end
  end
end
