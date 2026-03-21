require "mkmf"

abort "libcurl is required" unless have_library("curl", "curl_multi_init")
abort "curl/curl.h is required" unless have_header("curl/curl.h")

have_header("ruby/thread.h")
have_header("ruby/fiber/scheduler.h")

have_func("rb_fiber_scheduler_current", "ruby.h")
have_func("rb_io_wait", "ruby.h")

$CFLAGS << " -std=c99 -O2 -Wall -Wextra -Wno-unused-parameter"

create_makefile("fast_curl/fast_curl")
