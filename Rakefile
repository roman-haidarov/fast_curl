# frozen_string_literal: true

require "rake/extensiontask"
require "rake/testtask"

Rake::ExtensionTask.new("fast_curl") do |ext|
  ext.lib_dir = "lib/fast_curl"
end

Rake::TestTask.new(:test) do |t|
  t.libs << "test" << "lib"
  t.test_files = FileList["test/**/test_*.rb"]
end

task default: %i[compile test]
