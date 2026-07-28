# frozen_string_literal: true

require 'json'
require 'uri'
require_relative "fast_curl/version"

module FastCurl
  class Error < StandardError; end
  class TimeoutError < Error; end

  class Headers < Hash
    def [](key)
      super(normalize(key))
    end

    def []=(key, value)
      super(normalize(key), value)
    end
    alias store []=

    def fetch(key, *args, &block)
      super(normalize(key), *args, &block)
    end

    def key?(key)
      super(normalize(key))
    end
    alias has_key? key?
    alias include? key?
    alias member? key?

    def delete(key, &block)
      super(normalize(key), &block)
    end

    def dig(key, *rest)
      super(normalize(key), *rest)
    end

    def values_at(*keys)
      super(*keys.map { |key| normalize(key) })
    end

    def assoc(key)
      super(normalize(key))
    end

    def merge(*others, &block)
      others.inject(dup) { |acc, other| acc.merge!(other, &block) }
    end

    def merge!(*others)
      others.each do |other|
        other.each do |key, value|
          self[key] = block_given? && key?(key) ? yield(normalize(key), self[key], value) : value
        end
      end
      self
    end
    alias update merge!

    private

    def normalize(key)
      key.is_a?(String) || key.is_a?(Symbol) ? key.to_s.downcase : key
    end
  end
end

require_relative "fast_curl/fast_curl"

module FastCurl
  DEFAULT_OPTIONS = {
    connections: 20,
    timeout: 30
  }.freeze

  METHODS = %i[get post put delete patch].freeze
  BODY_METHODS = %i[post put patch].freeze

  JSON_TYPE = "application/json"
  FORM_TYPE = "application/x-www-form-urlencoded"
  BINARY_TYPE = "application/octet-stream"

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
        r = { url: build_url(req), method: method.to_s.upcase }
        headers = req[:headers] ? req[:headers].dup : nil

        if BODY_METHODS.include?(method)
          body, content_type = build_body(req)
          if body
            r[:body] = body
            if content_type && !content_type?(headers)
              headers ||= {}
              headers["Content-Type"] = content_type
            end
          end
        end

        r[:headers] = headers if headers && !headers.empty?
        r
      end
    end

    def build_url(req)
      url = req[:url]
      params = req[:params]
      return url if params.nil? || params.empty? || !url.is_a?(String)

      separator = url.include?("?") ? "&" : "?"
      "#{url}#{separator}#{URI.encode_www_form(params)}"
    end

    # Returns [body_string, default_content_type].
    #   json: {...} or "..."  -> application/json
    #   form: {...}           -> application/x-www-form-urlencoded
    #   body: "..."           -> application/octet-stream (set your own type)
    #   body: {...}           -> application/json (kept for backwards compatibility)
    def build_body(req)
      if req.key?(:json)
        value = req[:json]
        [value.is_a?(String) ? value : value.to_json, JSON_TYPE]
      elsif req.key?(:form)
        value = req[:form]
        [value.is_a?(String) ? value : URI.encode_www_form(value), FORM_TYPE]
      elsif req.key?(:body) && !req[:body].nil?
        value = req[:body]
        value.is_a?(Hash) ? [value.to_json, JSON_TYPE] : [value.to_s, BINARY_TYPE]
      end
    end

    def content_type?(headers)
      return false unless headers

      headers.any? { |k, _| k.to_s.casecmp("content-type").zero? }
    end
  end
end
