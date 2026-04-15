#include <ruby.h>
#include <ruby/io.h>
#include <ruby/thread.h>
#ifdef HAVE_RUBY_FIBER_SCHEDULER_H
#include <ruby/fiber/scheduler.h>
#endif
#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>

#define MAX_RESPONSE_SIZE     (100 * 1024 * 1024)
#define MAX_REDIRECTS         5
#define MAX_TIMEOUT           300
#define MAX_RETRIES           10
#define MAX_REQUESTS          10000
#define DEFAULT_RETRIES       1
#define DEFAULT_RETRY_DELAY   0
#define INITIAL_BUF_CAP       8192
#define INITIAL_HEADER_CAP    16
#define POLL_TIMEOUT_MS       50
#define FIBER_POLL_TIMEOUT_MS 10
#define HEADER_LINE_BUF_SIZE  512

static const CURLcode DEFAULT_RETRYABLE_CURLE[] = {
    CURLE_COULDNT_CONNECT, CURLE_OPERATION_TIMEDOUT, CURLE_SEND_ERROR,       CURLE_RECV_ERROR,
    CURLE_GOT_NOTHING,     CURLE_PARTIAL_FILE,       CURLE_SSL_CONNECT_ERROR};
#define DEFAULT_RETRYABLE_CURLE_COUNT \
    (int)(sizeof(DEFAULT_RETRYABLE_CURLE) / sizeof(DEFAULT_RETRYABLE_CURLE[0]))

static ID id_status, id_headers, id_body, id_error_code, id_url, id_method;
static ID id_timeout, id_connections, id_count, id_keys;
static ID id_retries, id_retry_delay, id_retry_codes;
static VALUE sym_status, sym_headers, sym_body, sym_error_code, sym_url, sym_method;
static VALUE sym_timeout, sym_connections, sym_count;
static VALUE sym_retries, sym_retry_delay, sym_retry_codes;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    size_t max_size;
} buffer_t;

static inline void buffer_init(buffer_t *buf) {
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
    buf->max_size = MAX_RESPONSE_SIZE;
}

static inline void buffer_free(buffer_t *buf) {
    if (buf->data) {
        free(buf->data);
        buf->data = NULL;
    }
    buf->len = 0;
    buf->cap = 0;
}

static inline void buffer_reset(buffer_t *buf) {
    buf->len = 0;
}

static size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    buffer_t *buf = (buffer_t *)userdata;
    size_t total = size * nmemb;
    if (buf->len + total > buf->max_size)
        return 0;
    if (buf->len + total >= buf->cap) {
        size_t new_cap = (buf->cap == 0) ? INITIAL_BUF_CAP : buf->cap;
        while (new_cap <= buf->len + total)
            new_cap *= 2;
        if (new_cap > buf->max_size)
            new_cap = buf->max_size;
        char *new_data = realloc(buf->data, new_cap);
        if (!new_data)
            return 0;
        buf->data = new_data;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, ptr, total);
    buf->len += total;
    return total;
}

typedef struct {
    char *str;
    size_t len;
} header_entry_t;

typedef struct {
    header_entry_t *entries;
    int count;
    int cap;
} header_list_t;

static inline void header_list_init(header_list_t *h) {
    h->entries = NULL;
    h->count = 0;
    h->cap = 0;
}

static void header_list_free(header_list_t *h) {
    for (int i = 0; i < h->count; i++)
        free(h->entries[i].str);
    free(h->entries);
    h->entries = NULL;
    h->count = 0;
    h->cap = 0;
}

static void header_list_reset(header_list_t *h) {
    for (int i = 0; i < h->count; i++)
        free(h->entries[i].str);
    h->count = 0;
}

static size_t header_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    header_list_t *h = (header_list_t *)userdata;
    size_t total = size * nmemb;
    if (total <= 2)
        return total;
    if (h->count >= h->cap) {
        int new_cap = (h->cap == 0) ? INITIAL_HEADER_CAP : h->cap * 2;
        header_entry_t *ne = realloc(h->entries, sizeof(header_entry_t) * new_cap);
        if (!ne)
            return 0;
        h->entries = ne;
        h->cap = new_cap;
    }
    size_t stripped = total;
    while (stripped > 0 && (ptr[stripped - 1] == '\r' || ptr[stripped - 1] == '\n'))
        stripped--;
    char *entry = malloc(stripped + 1);
    if (!entry)
        return 0;
    memcpy(entry, ptr, stripped);
    entry[stripped] = '\0';
    h->entries[h->count].str = entry;
    h->entries[h->count].len = stripped;
    h->count++;
    return size * nmemb;
}

typedef struct {
    CURL *easy;
    int index;
    buffer_t body;
    header_list_t headers;
    struct curl_slist *req_headers;
    int done;
    CURLcode curl_result;
    long http_status;
} request_ctx_t;

static inline void request_ctx_init(request_ctx_t *ctx, int index) {
    ctx->easy = curl_easy_init();
    ctx->index = index;
    buffer_init(&ctx->body);
    header_list_init(&ctx->headers);
    ctx->req_headers = NULL;
    ctx->done = 0;
    ctx->curl_result = CURLE_OK;
    ctx->http_status = 0;
}

static void request_ctx_free(request_ctx_t *ctx) {
    if (ctx->easy) {
        curl_easy_cleanup(ctx->easy);
        ctx->easy = NULL;
    }
    buffer_free(&ctx->body);
    header_list_free(&ctx->headers);
    if (ctx->req_headers) {
        curl_slist_free_all(ctx->req_headers);
        ctx->req_headers = NULL;
    }
}

static int request_ctx_reset_for_retry(request_ctx_t *ctx) {
    if (ctx->easy) {
        curl_easy_cleanup(ctx->easy);
        ctx->easy = NULL;
    }
    buffer_reset(&ctx->body);
    header_list_reset(&ctx->headers);
    if (ctx->req_headers) {
        curl_slist_free_all(ctx->req_headers);
        ctx->req_headers = NULL;
    }
    ctx->easy = curl_easy_init();
    if (!ctx->easy)
        return 0;
    ctx->done = 0;
    ctx->curl_result = CURLE_OK;
    ctx->http_status = 0;
    return 1;
}

typedef struct {
    CURLM *multi;
    request_ctx_t *requests;
    int count;
    int still_running;
    long timeout_ms;
    int max_connections;
    volatile int cancelled;
} multi_session_t;

typedef struct {
    int max_retries;
    long retry_delay_ms;
    int *retry_http_codes;
    int retry_http_count;
} retry_config_t;

static int contains_header_injection(const char *str, long len) {
    for (long i = 0; i < len; i++) {
        if (str[i] == '\r' || str[i] == '\n' || str[i] == '\0')
            return 1;
    }
    return 0;
}

#ifdef HAVE_RB_FIBER_SCHEDULER_CURRENT
static VALUE current_fiber_scheduler(void) {
    VALUE sched = rb_fiber_scheduler_current();
    if (sched == Qnil || sched == Qfalse)
        return Qnil;
    return sched;
}
#else
static VALUE current_fiber_scheduler(void) {
    return Qnil;
}
#endif

typedef struct {
    void *(*func)(void *);
    void *arg;
    VALUE scheduler;
    VALUE blocker;
    VALUE fiber;
} fiber_worker_ctx_t;

static void *fiber_worker_nogvl(void *arg) {
    fiber_worker_ctx_t *c = (fiber_worker_ctx_t *)arg;
    c->func(c->arg);
    return NULL;
}

static VALUE fiber_worker_thread(void *arg) {
    fiber_worker_ctx_t *c = (fiber_worker_ctx_t *)arg;
    rb_thread_call_without_gvl(fiber_worker_nogvl, c, RUBY_UBF_PROCESS, NULL);
    rb_fiber_scheduler_unblock(c->scheduler, c->blocker, c->fiber);
    return Qnil;
}

static void run_via_fiber_worker(VALUE scheduler, void *(*func)(void *), void *arg) {
    fiber_worker_ctx_t ctx = {
        .func = func,
        .arg = arg,
        .scheduler = scheduler,
        .blocker = rb_obj_alloc(rb_cObject),
        .fiber = rb_fiber_current(),
    };
    VALUE th = rb_thread_create(fiber_worker_thread, &ctx);
    rb_fiber_scheduler_block(scheduler, ctx.blocker, Qnil);
    rb_funcall(th, rb_intern("join"), 0);
}

static VALUE build_response(request_ctx_t *ctx) {
    long status = 0;
    curl_easy_getinfo(ctx->easy, CURLINFO_RESPONSE_CODE, &status);
    VALUE headers_hash = rb_hash_new();
    for (int i = 0; i < ctx->headers.count; i++) {
        const char *hdr = ctx->headers.entries[i].str;
        size_t hdr_len = ctx->headers.entries[i].len;
        const char *colon = memchr(hdr, ':', hdr_len);
        if (!colon)
            continue;
        VALUE key = rb_str_new(hdr, colon - hdr);
        const char *vs = colon + 1, *ve = hdr + hdr_len;
        while (vs < ve && (*vs == ' ' || *vs == '\t'))
            vs++;
        while (ve > vs && (*(ve - 1) == ' ' || *(ve - 1) == '\t'))
            ve--;
        VALUE val = rb_str_new(vs, ve - vs);
        rb_hash_aset(headers_hash, key, val);
    }
    VALUE body_str =
        ctx->body.data ? rb_str_new(ctx->body.data, ctx->body.len) : rb_str_new_cstr("");
    VALUE result = rb_hash_new();
    rb_hash_aset(result, sym_status, LONG2NUM(status));
    rb_hash_aset(result, sym_headers, headers_hash);
    rb_hash_aset(result, sym_body, body_str);
    return result;
}

static VALUE build_error_response(const char *message) {
    VALUE r = rb_hash_new();
    rb_hash_aset(r, sym_status, INT2NUM(0));
    rb_hash_aset(r, sym_headers, Qnil);
    rb_hash_aset(r, sym_body, rb_str_new_cstr(message));
    return r;
}

static VALUE build_error_response_with_code(const char *message, int error_code) {
    VALUE r = rb_hash_new();
    rb_hash_aset(r, sym_status, INT2NUM(0));
    rb_hash_aset(r, sym_headers, Qnil);
    rb_hash_aset(r, sym_body, rb_str_new_cstr(message));
    rb_hash_aset(r, sym_error_code, INT2NUM(error_code));
    return r;
}

static int is_valid_url(const char *url) {
    if (!url)
        return 0;
    size_t len = strlen(url);
    if (len < 8 || len > 2048)
        return 0;
    if (strncmp(url, "https://", 8) == 0)
        return 1;
    if (len >= 7 && strncmp(url, "http://", 7) == 0)
        return 1;
    return 0;
}

#define CURL_SETOPT_CHECK(handle, option, value)               \
    do {                                                       \
        CURLcode _r = curl_easy_setopt(handle, option, value); \
        if (_r != CURLE_OK)                                    \
            return _r;                                         \
    } while (0)

static CURLcode setup_basic_options(CURL *easy, const char *url_str, long timeout_sec,
                                    request_ctx_t *ctx) {
    CURL_SETOPT_CHECK(easy, CURLOPT_URL, url_str);
    CURL_SETOPT_CHECK(easy, CURLOPT_WRITEFUNCTION, write_callback);
    CURL_SETOPT_CHECK(easy, CURLOPT_WRITEDATA, &ctx->body);
    CURL_SETOPT_CHECK(easy, CURLOPT_HEADERFUNCTION, header_callback);
    CURL_SETOPT_CHECK(easy, CURLOPT_HEADERDATA, &ctx->headers);
    CURL_SETOPT_CHECK(easy, CURLOPT_TIMEOUT, timeout_sec);
    CURL_SETOPT_CHECK(easy, CURLOPT_NOSIGNAL, 1L);
    CURL_SETOPT_CHECK(easy, CURLOPT_FOLLOWLOCATION, 1L);
    CURL_SETOPT_CHECK(easy, CURLOPT_MAXREDIRS, MAX_REDIRECTS);
    CURL_SETOPT_CHECK(easy, CURLOPT_ACCEPT_ENCODING, "");
    CURL_SETOPT_CHECK(easy, CURLOPT_PRIVATE, (char *)ctx);
    CURL_SETOPT_CHECK(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    return CURLE_OK;
}

static CURLcode setup_security_options(CURL *easy) {
    CURL_SETOPT_CHECK(easy, CURLOPT_SSL_VERIFYPEER, 1L);
    CURL_SETOPT_CHECK(easy, CURLOPT_SSL_VERIFYHOST, 2L);
#ifdef CURLOPT_PROTOCOLS_STR
    CURL_SETOPT_CHECK(easy, CURLOPT_PROTOCOLS_STR, "http,https");
    CURL_SETOPT_CHECK(easy, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
    CURL_SETOPT_CHECK(easy, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
    CURL_SETOPT_CHECK(easy, CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#endif
    return CURLE_OK;
}

static CURLcode setup_method_and_body(CURL *easy, VALUE method, VALUE body) {
    if (!NIL_P(method)) {
        const char *m = StringValueCStr(method);
        if (strcmp(m, "POST") == 0) {
            CURL_SETOPT_CHECK(easy, CURLOPT_POST, 1L);
        } else if (strcmp(m, "PUT") == 0) {
            CURL_SETOPT_CHECK(easy, CURLOPT_CUSTOMREQUEST, "PUT");
        } else if (strcmp(m, "DELETE") == 0) {
            CURL_SETOPT_CHECK(easy, CURLOPT_CUSTOMREQUEST, "DELETE");
        } else if (strcmp(m, "PATCH") == 0) {
            CURL_SETOPT_CHECK(easy, CURLOPT_CUSTOMREQUEST, "PATCH");
        } else if (strcmp(m, "GET") != 0) {
            CURL_SETOPT_CHECK(easy, CURLOPT_CUSTOMREQUEST, m);
        }
    }

    if (!NIL_P(body)) {
        CURL_SETOPT_CHECK(easy, CURLOPT_POSTFIELDSIZE, RSTRING_LEN(body));
        CURL_SETOPT_CHECK(easy, CURLOPT_COPYPOSTFIELDS, StringValuePtr(body));
    }
    return CURLE_OK;
}

static int header_iter_cb(VALUE key, VALUE val, VALUE arg) {
    request_ctx_t *ctx = (request_ctx_t *)arg;
    VALUE key_str = rb_String(key);
    const char *k = RSTRING_PTR(key_str);
    long klen = RSTRING_LEN(key_str);

    if (contains_header_injection(k, klen))
        return ST_CONTINUE;

    if (NIL_P(val) || RSTRING_LEN(rb_String(val)) == 0) {
        char sbuf[HEADER_LINE_BUF_SIZE];
        char *buf = sbuf;
        long need = klen + 2;
        if (need > HEADER_LINE_BUF_SIZE)
            buf = malloc(need);
        if (!buf)
            return ST_CONTINUE;
        memcpy(buf, k, klen);
        buf[klen] = ';';
        buf[klen + 1] = '\0';
        ctx->req_headers = curl_slist_append(ctx->req_headers, buf);
        if (buf != sbuf)
            free(buf);
    } else {
        VALUE val_str = rb_String(val);
        const char *v = RSTRING_PTR(val_str);
        long vlen = RSTRING_LEN(val_str);

        if (contains_header_injection(v, vlen))
            return ST_CONTINUE;
        char sbuf[HEADER_LINE_BUF_SIZE];
        char *buf = sbuf;
        long need = klen + 2 + vlen + 1;
        if (need > HEADER_LINE_BUF_SIZE)
            buf = malloc(need);
        if (!buf)
            return ST_CONTINUE;

        memcpy(buf, k, klen);
        buf[klen] = ':';
        buf[klen + 1] = ' ';
        memcpy(buf + klen + 2, v, vlen);
        buf[klen + 2 + vlen] = '\0';
        ctx->req_headers = curl_slist_append(ctx->req_headers, buf);
        if (buf != sbuf)
            free(buf);
    }
    return ST_CONTINUE;
}

static int setup_easy_handle(request_ctx_t *ctx, VALUE request, long timeout_sec) {
    VALUE url = rb_hash_aref(request, sym_url);
    VALUE method = rb_hash_aref(request, sym_method);
    VALUE headers = rb_hash_aref(request, sym_headers);
    VALUE body = rb_hash_aref(request, sym_body);
    if (NIL_P(url))
        return 0;
    const char *url_str = StringValueCStr(url);
    if (!is_valid_url(url_str))
        rb_raise(rb_eArgError, "Invalid URL: %s", url_str);

    CURLcode res;
    res = setup_basic_options(ctx->easy, url_str, timeout_sec, ctx);
    if (res != CURLE_OK)
        return 0;
    res = setup_security_options(ctx->easy);
    if (res != CURLE_OK)
        return 0;
    res = setup_method_and_body(ctx->easy, method, body);
    if (res != CURLE_OK)
        return 0;

    if (!NIL_P(headers) && rb_obj_is_kind_of(headers, rb_cHash)) {
        rb_hash_foreach(headers, header_iter_cb, (VALUE)ctx);
        if (ctx->req_headers) {
            res = curl_easy_setopt(ctx->easy, CURLOPT_HTTPHEADER, ctx->req_headers);
            if (res != CURLE_OK)
                return 0;
        }
    }

    RB_GC_GUARD(url);
    RB_GC_GUARD(method);
    RB_GC_GUARD(headers);
    RB_GC_GUARD(body);
    RB_GC_GUARD(request);
    return 1;
}

static void *poll_without_gvl(void *arg) {
    multi_session_t *s = (multi_session_t *)arg;
    if (s->cancelled)
        return NULL;
    int numfds = 0;
    curl_multi_poll(s->multi, NULL, 0, POLL_TIMEOUT_MS, &numfds);
    curl_multi_perform(s->multi, &s->still_running);
    return NULL;
}

static void unblock_perform(void *arg) {
    multi_session_t *s = (multi_session_t *)arg;
    s->cancelled = 1;
#ifdef HAVE_CURL_MULTI_WAKEUP
    curl_multi_wakeup(s->multi);
#endif
}

typedef struct {
    VALUE results;
    int completed;
    int target;
    int stream;
} completion_ctx_t;

static int process_completed(multi_session_t *session, completion_ctx_t *cctx) {
    CURLMsg *msg;
    int msgs_left;
    while ((msg = curl_multi_info_read(session->multi, &msgs_left))) {
        if (msg->msg != CURLMSG_DONE)
            continue;
        request_ctx_t *ctx = NULL;
        curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (char **)&ctx);
        if (!ctx || ctx->done)
            continue;
        ctx->done = 1;
        ctx->curl_result = msg->data.result;
        if (msg->data.result == CURLE_OK)
            curl_easy_getinfo(ctx->easy, CURLINFO_RESPONSE_CODE, &ctx->http_status);
        if (cctx->stream) {
            VALUE response = (msg->data.result == CURLE_OK)
                                 ? build_response(ctx)
                                 : build_error_response_with_code(
                                       curl_easy_strerror(msg->data.result), (int)msg->data.result);
            VALUE pair = rb_ary_new_from_args(2, INT2NUM(ctx->index), response);
            rb_yield(pair);
            cctx->completed++;
        } else {
            cctx->completed++;
        }
        if (cctx->target > 0 && cctx->completed >= cctx->target)
            return 1;
    }
    return 0;
}

static void run_multi_loop(multi_session_t *session, completion_ctx_t *cctx) {
    VALUE scheduler = current_fiber_scheduler();

    if (scheduler != Qnil) {
        curl_multi_perform(session->multi, &session->still_running);
        while (session->still_running > 0) {
            if (session->cancelled)
                break;
            run_via_fiber_worker(scheduler, poll_without_gvl, session);
            if (process_completed(session, cctx))
                break;
        }
        process_completed(session, cctx);
    } else {
        curl_multi_perform(session->multi, &session->still_running);
        while (session->still_running > 0) {
            if (session->cancelled)
                break;
            rb_thread_call_without_gvl(poll_without_gvl, session, unblock_perform, session);
            if (process_completed(session, cctx))
                break;
        }
        process_completed(session, cctx);
    }
}

static int is_default_retryable_curle(CURLcode code) {
    for (int i = 0; i < DEFAULT_RETRYABLE_CURLE_COUNT; i++)
        if (DEFAULT_RETRYABLE_CURLE[i] == code)
            return 1;
    return 0;
}

static int should_retry(request_ctx_t *ctx, retry_config_t *rc) {
    if (ctx->curl_result != CURLE_OK)
        return is_default_retryable_curle(ctx->curl_result);
    for (int i = 0; i < rc->retry_http_count; i++)
        if (rc->retry_http_codes[i] == (int)ctx->http_status)
            return 1;
    return 0;
}

typedef struct {
    long delay_ms;
} sleep_arg_t;

static void *sleep_without_gvl(void *arg) {
    sleep_arg_t *sa = (sleep_arg_t *)arg;
    struct timespec ts = {.tv_sec = sa->delay_ms / 1000,
                          .tv_nsec = (sa->delay_ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
    return NULL;
}

/* FIX #2: Fiber path releases GVL via run_via_fiber_worker */
static void retry_delay_sleep(long delay_ms) {
    if (delay_ms <= 0)
        return;
    VALUE scheduler = current_fiber_scheduler();
    if (scheduler != Qnil) {
        long remaining = delay_ms;
        while (remaining > 0) {
            long chunk = remaining > FIBER_POLL_TIMEOUT_MS ? FIBER_POLL_TIMEOUT_MS : remaining;
            sleep_arg_t sa = {.delay_ms = chunk};
            run_via_fiber_worker(scheduler, sleep_without_gvl, &sa);
            remaining -= chunk;
        }
    } else {
        sleep_arg_t sa = {.delay_ms = delay_ms};
        rb_thread_call_without_gvl(sleep_without_gvl, &sa, (rb_unblock_function_t *)0, NULL);
    }
}

static void parse_options(VALUE options, long *timeout, int *max_conn, retry_config_t *retry_cfg) {
    *timeout = 30;
    *max_conn = 20;
    retry_cfg->max_retries = DEFAULT_RETRIES;
    retry_cfg->retry_delay_ms = DEFAULT_RETRY_DELAY;
    retry_cfg->retry_http_codes = NULL;
    retry_cfg->retry_http_count = 0;
    if (NIL_P(options) || !rb_obj_is_kind_of(options, rb_cHash))
        return;

    VALUE t = rb_hash_aref(options, sym_timeout);
    if (!NIL_P(t)) {
        long v = NUM2LONG(t);
        if (v > MAX_TIMEOUT)
            v = MAX_TIMEOUT;
        else if (v <= 0)
            v = 30;
        *timeout = v;
    }
    VALUE c = rb_hash_aref(options, sym_connections);
    if (!NIL_P(c)) {
        int v = NUM2INT(c);
        if (v > 100)
            v = 100;
        else if (v <= 0)
            v = 20;
        *max_conn = v;
    }
    VALUE r = rb_hash_aref(options, sym_retries);
    if (!NIL_P(r)) {
        int v = NUM2INT(r);
        if (v < 0)
            v = 0;
        if (v > MAX_RETRIES)
            v = MAX_RETRIES;
        retry_cfg->max_retries = v;
    }
    VALUE rd = rb_hash_aref(options, sym_retry_delay);
    if (!NIL_P(rd)) {
        long v = NUM2LONG(rd);
        if (v < 0)
            v = 0;
        if (v > 30000)
            v = 30000;
        retry_cfg->retry_delay_ms = v;
    }
    VALUE rc = rb_hash_aref(options, sym_retry_codes);
    if (!NIL_P(rc) && rb_obj_is_kind_of(rc, rb_cArray)) {
        int len = (int)RARRAY_LEN(rc);
        if (len > 0) {
            retry_cfg->retry_http_codes = malloc(sizeof(int) * len);
            if (retry_cfg->retry_http_codes) {
                retry_cfg->retry_http_count = len;
                for (int i = 0; i < len; i++)
                    retry_cfg->retry_http_codes[i] = NUM2INT(rb_ary_entry(rc, i));
            }
        }
    }
}

typedef struct {
    multi_session_t *session;
    int *invalid;
    retry_config_t *retry_cfg;
} cleanup_ctx_t;

static VALUE cleanup_session(VALUE arg) {
    cleanup_ctx_t *ctx = (cleanup_ctx_t *)arg;
    if (ctx->session->requests) {
        for (int i = 0; i < ctx->session->count; i++) {
            if (ctx->session->requests[i].easy)
                curl_multi_remove_handle(ctx->session->multi, ctx->session->requests[i].easy);
            request_ctx_free(&ctx->session->requests[i]);
        }
        free(ctx->session->requests);
        ctx->session->requests = NULL;
    }
    if (ctx->invalid) {
        free(ctx->invalid);
        ctx->invalid = NULL;
    }
    if (ctx->session->multi) {
        curl_multi_cleanup(ctx->session->multi);
        ctx->session->multi = NULL;
    }
    if (ctx->retry_cfg && ctx->retry_cfg->retry_http_codes) {
        free(ctx->retry_cfg->retry_http_codes);
        ctx->retry_cfg->retry_http_codes = NULL;
    }
    return Qnil;
}

typedef struct {
    VALUE requests;
    VALUE options;
    int target;
    int stream;
    multi_session_t *session;
    int *invalid;
    retry_config_t *retry_cfg;
    long timeout_sec;
} execute_args_t;

static VALUE internal_execute_body(VALUE arg) {
    execute_args_t *ea = (execute_args_t *)arg;
    VALUE requests = ea->requests;
    multi_session_t *session = ea->session;
    int *invalid = ea->invalid;
    retry_config_t *retry_cfg = ea->retry_cfg;
    long timeout_sec = ea->timeout_sec;
    int count = session->count, target = ea->target, stream = ea->stream;

    int valid_requests = 0;
    for (int i = 0; i < count; i++) {
        VALUE req = rb_ary_entry(requests, i);
        request_ctx_init(&session->requests[i], i);
        if (!setup_easy_handle(&session->requests[i], req, timeout_sec)) {
            session->requests[i].done = 1;
            invalid[i] = 1;
            continue;
        }
        CURLMcode mc = curl_multi_add_handle(session->multi, session->requests[i].easy);
        if (mc != CURLM_OK) {
            session->requests[i].done = 1;
            invalid[i] = 1;
            continue;
        }
        valid_requests++;
    }
    if (valid_requests == 0)
        session->still_running = 0;

    completion_ctx_t cctx;
    cctx.results = stream ? Qnil : rb_ary_new2(count);
    cctx.completed = 0;
    cctx.target = target;
    cctx.stream = stream;
    if (!stream) {
        for (int i = 0; i < count; i++)
            rb_ary_store(cctx.results, i, Qnil);
    }

    run_multi_loop(session, &cctx);

    if (!stream && retry_cfg->max_retries > 0) {
        int prev_all_failed = 0;
        for (int attempt = 0; attempt < retry_cfg->max_retries; attempt++) {
            int retry_count = 0;
            int *ri = malloc(sizeof(int) * count);
            if (!ri)
                break;
            for (int i = 0; i < count; i++) {
                if (invalid[i] || !session->requests[i].done)
                    continue;
                if (should_retry(&session->requests[i], retry_cfg))
                    ri[retry_count++] = i;
            }
            if (retry_count == 0) {
                free(ri);
                break;
            }
            int done_count = 0;
            for (int i = 0; i < count; i++)
                if (!invalid[i] && session->requests[i].done)
                    done_count++;
            int all_failed = (retry_count == done_count);
            if (all_failed && prev_all_failed) {
                free(ri);
                break;
            }
            prev_all_failed = all_failed;
            retry_delay_sleep(retry_cfg->retry_delay_ms);
            for (int r = 0; r < retry_count; r++) {
                int idx = ri[r];
                request_ctx_t *rc = &session->requests[idx];
                curl_multi_remove_handle(session->multi, rc->easy);
                if (!request_ctx_reset_for_retry(rc)) {
                    rc->done = 1;
                    invalid[idx] = 1;
                    continue;
                }
                VALUE req = rb_ary_entry(requests, idx);
                if (!setup_easy_handle(rc, req, timeout_sec)) {
                    rc->done = 1;
                    invalid[idx] = 1;
                    continue;
                }
                CURLMcode mc = curl_multi_add_handle(session->multi, rc->easy);
                if (mc != CURLM_OK) {
                    rc->done = 1;
                    invalid[idx] = 1;
                }
            }
            free(ri);
            cctx.completed = 0;
            run_multi_loop(session, &cctx);
        }
    }

    if (!stream) {
        for (int i = 0; i < count; i++) {
            request_ctx_t *rc = &session->requests[i];
            VALUE response;
            if (invalid[i]) {
                response = build_error_response("Invalid request configuration");
            } else if (rc->curl_result == CURLE_OK) {
                response = build_response(rc);
            } else {
                response = build_error_response_with_code(curl_easy_strerror(rc->curl_result),
                                                          (int)rc->curl_result);
            }
            rb_ary_store(cctx.results, i, rb_ary_new_from_args(2, INT2NUM(i), response));
        }
    }
    return stream ? Qnil : cctx.results;
}

static VALUE internal_execute(VALUE requests, VALUE options, int target, int stream) {
    Check_Type(requests, T_ARRAY);

    long count_long = RARRAY_LEN(requests);
    if (count_long == 0)
        return rb_ary_new();
    if (count_long > MAX_REQUESTS)
        rb_raise(rb_eArgError, "too many requests (%ld), maximum is %d", count_long, MAX_REQUESTS);
    if (count_long > INT_MAX)
        rb_raise(rb_eArgError, "request count overflows int");
    int count = (int)count_long;

    long timeout_sec;
    int max_conn;
    retry_config_t retry_cfg;
    parse_options(options, &timeout_sec, &max_conn, &retry_cfg);

    if (stream || target > 0) {
        if (retry_cfg.max_retries > 0 && stream)
            rb_warn("FastCurl: retries are not supported in stream_execute, ignoring "
                    "retries option");
        if (retry_cfg.max_retries > 0 && target > 0)
            rb_warn("FastCurl: retries are not supported in first_execute, ignoring "
                    "retries option");
        retry_cfg.max_retries = 0;
    }

    multi_session_t session;
    session.multi = curl_multi_init();
    session.count = count;
    session.timeout_ms = timeout_sec * 1000;
    session.max_connections = max_conn;
    session.cancelled = 0;
    session.requests = NULL;

    curl_multi_setopt(session.multi, CURLMOPT_MAXCONNECTS, (long)max_conn);
    curl_multi_setopt(session.multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, (long)max_conn);
#ifdef CURLPIPE_MULTIPLEX
    curl_multi_setopt(session.multi, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif

    session.requests = calloc(count, sizeof(request_ctx_t));
    if (!session.requests) {
        curl_multi_cleanup(session.multi);
        if (retry_cfg.retry_http_codes)
            free(retry_cfg.retry_http_codes);
        rb_raise(rb_eNoMemError, "failed to allocate request contexts");
    }

    int *invalid = calloc(count, sizeof(int));
    if (!invalid) {
        free(session.requests);
        curl_multi_cleanup(session.multi);
        if (retry_cfg.retry_http_codes)
            free(retry_cfg.retry_http_codes);
        rb_raise(rb_eNoMemError, "failed to allocate tracking array");
    }

    cleanup_ctx_t cleanup = {.session = &session, .invalid = invalid, .retry_cfg = &retry_cfg};
    execute_args_t ea = {
        .requests = requests,
        .options = options,
        .target = target,
        .stream = stream,
        .session = &session,
        .invalid = invalid,
        .retry_cfg = &retry_cfg,
        .timeout_sec = timeout_sec,
    };
    return rb_ensure(internal_execute_body, (VALUE)&ea, cleanup_session, (VALUE)&cleanup);
}

static VALUE rb_fast_curl_execute(int argc, VALUE *argv, VALUE self) {
    VALUE requests, options;
    rb_scan_args(argc, argv, "1:", &requests, &options);
    return internal_execute(requests, options, -1, 0);
}

static VALUE rb_fast_curl_first_execute(int argc, VALUE *argv, VALUE self) {
    VALUE requests, options;
    rb_scan_args(argc, argv, "1:", &requests, &options);
    int count = 1;
    if (!NIL_P(options)) {
        VALUE c = rb_hash_aref(options, sym_count);
        if (!NIL_P(c))
            count = NUM2INT(c);
    }
    return internal_execute(requests, options, count, 0);
}

static VALUE rb_fast_curl_stream_execute(int argc, VALUE *argv, VALUE self) {
    VALUE requests, options;
    rb_scan_args(argc, argv, "1:", &requests, &options);
    if (!rb_block_given_p())
        rb_raise(rb_eArgError, "stream_execute requires a block");
    return internal_execute(requests, options, -1, 1);
}

void Init_fast_curl(void) {
    curl_global_init(CURL_GLOBAL_ALL);

    id_status = rb_intern("status");
    id_headers = rb_intern("headers");
    id_body = rb_intern("body");
    id_error_code = rb_intern("error_code");
    id_url = rb_intern("url");
    id_method = rb_intern("method");
    id_timeout = rb_intern("timeout");
    id_connections = rb_intern("connections");
    id_count = rb_intern("count");
    id_keys = rb_intern("keys");
    id_retries = rb_intern("retries");
    id_retry_delay = rb_intern("retry_delay");
    id_retry_codes = rb_intern("retry_codes");

    sym_status = ID2SYM(id_status);
    rb_gc_register_address(&sym_status);
    sym_headers = ID2SYM(id_headers);
    rb_gc_register_address(&sym_headers);
    sym_body = ID2SYM(id_body);
    rb_gc_register_address(&sym_body);
    sym_error_code = ID2SYM(id_error_code);
    rb_gc_register_address(&sym_error_code);
    sym_url = ID2SYM(id_url);
    rb_gc_register_address(&sym_url);
    sym_method = ID2SYM(id_method);
    rb_gc_register_address(&sym_method);
    sym_timeout = ID2SYM(id_timeout);
    rb_gc_register_address(&sym_timeout);
    sym_connections = ID2SYM(id_connections);
    rb_gc_register_address(&sym_connections);
    sym_count = ID2SYM(id_count);
    rb_gc_register_address(&sym_count);
    sym_retries = ID2SYM(id_retries);
    rb_gc_register_address(&sym_retries);
    sym_retry_delay = ID2SYM(id_retry_delay);
    rb_gc_register_address(&sym_retry_delay);
    sym_retry_codes = ID2SYM(id_retry_codes);
    rb_gc_register_address(&sym_retry_codes);

    VALUE mFastCurl = rb_define_module("FastCurl");

    rb_define_module_function(mFastCurl, "execute", rb_fast_curl_execute, -1);
    rb_define_module_function(mFastCurl, "first_execute", rb_fast_curl_first_execute, -1);
    rb_define_module_function(mFastCurl, "stream_execute", rb_fast_curl_stream_execute, -1);
}
