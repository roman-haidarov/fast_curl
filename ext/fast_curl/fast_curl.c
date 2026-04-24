#include <ruby.h>
#include <ruby/io.h>
#include <ruby/thread.h>
#ifdef HAVE_RUBY_FIBER_SCHEDULER_H
#include <ruby/fiber/scheduler.h>
#endif

#if defined(HAVE_RUBY_FIBER_SCHEDULER_H) && defined(HAVE_RB_FIBER_SCHEDULER_CURRENT) &&   \
    defined(HAVE_RB_FIBER_SCHEDULER_BLOCK) && defined(HAVE_RB_FIBER_SCHEDULER_UNBLOCK) && \
    defined(HAVE_RB_FIBER_CURRENT)
#define FAST_CURL_HAVE_FIBER_SCHEDULER 1
#endif
#include <curl/curl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#if defined(__GNUC__) || defined(__clang__)
#define FAST_CURL_NORETURN __attribute__((noreturn))
#else
#define FAST_CURL_NORETURN
#endif

#define MAX_RESPONSE_SIZE     (100 * 1024 * 1024)
#define MAX_REDIRECTS         5
#define MAX_TIMEOUT           300
#define MAX_RETRIES           10
#define MAX_REQUESTS          10000
#define MAX_CONNECTIONS       100
#define MAX_RETRY_DELAY_MS    30000
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

typedef enum {
    KEY_STATUS,
    KEY_HEADERS,
    KEY_BODY,
    KEY_ERROR_CODE,
    KEY_URL,
    KEY_METHOD,
    KEY_TIMEOUT,
    KEY_CONNECTIONS,
    KEY_COUNT_OPT,
    KEY_RETRIES,
    KEY_RETRY_DELAY,
    KEY_RETRY_CODES,
    KEY_LAST
} key_id_t;

static ID fast_ids[KEY_LAST];
static VALUE fast_syms[KEY_LAST];

#define SYM(key) fast_syms[key]

static const char *const KEY_NAMES[KEY_LAST] = {
    "status",  "headers",     "body",  "error_code", "url",         "method",
    "timeout", "connections", "count", "retries",    "retry_delay", "retry_codes",
};

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

    if (nmemb != 0 && total / nmemb != size)
        return 0;
    if (total > buf->max_size || buf->len > buf->max_size - total)
        return 0;

    if (buf->len + total >= buf->cap) {
        size_t new_cap = (buf->cap == 0) ? INITIAL_BUF_CAP : buf->cap;
        while (new_cap <= buf->len + total) {
            if (new_cap > buf->max_size / 2) {
                new_cap = buf->max_size;
                break;
            }
            new_cap *= 2;
        }
        if (new_cap < buf->len + total)
            return 0;

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

    if (nmemb != 0 && total / nmemb != size)
        return 0;

    size_t stripped = total;
    while (stripped > 0 && (ptr[stripped - 1] == '\r' || ptr[stripped - 1] == '\n'))
        stripped--;

    if (stripped >= 5 && memcmp(ptr, "HTTP/", 5) == 0) {
        header_list_reset(h);
        return total;
    }

    if (stripped == 0)
        return total;

    if (h->count >= h->cap) {
        int new_cap = (h->cap == 0) ? INITIAL_HEADER_CAP : h->cap * 2;
        header_entry_t *ne = realloc(h->entries, sizeof(header_entry_t) * new_cap);
        if (!ne)
            return 0;
        h->entries = ne;
        h->cap = new_cap;
    }

    char *entry = malloc(stripped + 1);
    if (!entry)
        return 0;
    memcpy(entry, ptr, stripped);
    entry[stripped] = '\0';

    h->entries[h->count].str = entry;
    h->entries[h->count].len = stripped;
    h->count++;
    return total;
}

typedef struct {
    CURL *easy;
    int index;
    buffer_t body;
    header_list_t headers;
    struct curl_slist *req_headers;
    int done;
    int active;
    CURLcode curl_result;
    long http_status;
} request_ctx_t;

static inline void request_ctx_init(request_ctx_t *ctx, int index) {
    ctx->easy = NULL;
    ctx->index = index;
    buffer_init(&ctx->body);
    header_list_init(&ctx->headers);
    ctx->req_headers = NULL;
    ctx->done = 0;
    ctx->active = 0;
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
    ctx->active = 0;
}

static int request_ctx_prepare_easy(request_ctx_t *ctx) {
    if (!ctx->easy) {
        ctx->easy = curl_easy_init();
        if (!ctx->easy)
            return 0;
    }
    return 1;
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
    ctx->active = 0;
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

    int active_count;
    int pending_pos;
    int pending_count;
    int *pending_indices;
} multi_session_t;

typedef struct {
    int max_retries;
    int retries_explicit;
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

static int is_header_token_char(unsigned char c) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))
        return 1;
    switch (c) {
    case '!':
    case '#':
    case '$':
    case '%':
    case '&':
    case '\'':
    case '*':
    case '+':
    case '-':
    case '.':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
        return 1;
    default:
        return 0;
    }
}

static int is_valid_header_name(const char *str, long len) {
    if (len <= 0)
        return 0;
    for (long i = 0; i < len; i++) {
        if (!is_header_token_char((unsigned char)str[i]))
            return 0;
    }
    return 1;
}

static VALUE hash_aref_symbol_or_string(VALUE hash, VALUE sym, ID id) {
    VALUE value = rb_hash_aref(hash, sym);
    if (!NIL_P(value))
        return value;

    const char *name = rb_id2name(id);
    return name ? rb_hash_aref(hash, rb_str_new_cstr(name)) : Qnil;
}

static inline VALUE hash_aref_key(VALUE hash, key_id_t key) {
    return hash_aref_symbol_or_string(hash, fast_syms[key], fast_ids[key]);
}

#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
static VALUE current_fiber_scheduler(void) {
    VALUE sched = rb_fiber_scheduler_current();
    if (sched == Qnil || sched == Qfalse)
        return Qnil;
    return sched;
}

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
#endif

static void headers_hash_store(VALUE headers_hash, VALUE key, VALUE val) {
    VALUE existing = rb_hash_aref(headers_hash, key);

    if (NIL_P(existing)) {
        rb_hash_aset(headers_hash, key, val);
    } else if (RB_TYPE_P(existing, T_ARRAY)) {
        rb_ary_push(existing, val);
    } else {
        VALUE values = rb_ary_new_from_args(2, existing, val);
        rb_hash_aset(headers_hash, key, values);
    }
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
        const char *vs = colon + 1;
        const char *ve = hdr + hdr_len;

        while (vs < ve && (*vs == ' ' || *vs == '\t'))
            vs++;
        while (ve > vs && (*(ve - 1) == ' ' || *(ve - 1) == '\t'))
            ve--;

        VALUE val = rb_str_new(vs, ve - vs);
        headers_hash_store(headers_hash, key, val);
    }

    VALUE body_str =
        ctx->body.data ? rb_str_new(ctx->body.data, ctx->body.len) : rb_str_new_cstr("");

    VALUE result = rb_hash_new();
    rb_hash_aset(result, SYM(KEY_STATUS), LONG2NUM(status));
    rb_hash_aset(result, SYM(KEY_HEADERS), headers_hash);
    rb_hash_aset(result, SYM(KEY_BODY), body_str);
    return result;
}

static VALUE build_error_response(const char *message) {
    VALUE r = rb_hash_new();
    rb_hash_aset(r, SYM(KEY_STATUS), INT2NUM(0));
    rb_hash_aset(r, SYM(KEY_HEADERS), Qnil);
    rb_hash_aset(r, SYM(KEY_BODY), rb_str_new_cstr(message));
    return r;
}

static VALUE build_error_response_with_code(const char *message, int error_code) {
    VALUE r = build_error_response(message);
    rb_hash_aset(r, SYM(KEY_ERROR_CODE), INT2NUM(error_code));
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
#if LIBCURL_VERSION_NUM >= 0x075500
    CURL_SETOPT_CHECK(easy, CURLOPT_PROTOCOLS_STR, "http,https");
    CURL_SETOPT_CHECK(easy, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
    CURL_SETOPT_CHECK(easy, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
    CURL_SETOPT_CHECK(easy, CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#endif
    return CURLE_OK;
}

static CURLcode set_body(CURL *easy, VALUE body) {
    VALUE body_str = rb_String(body);
    CURL_SETOPT_CHECK(easy, CURLOPT_POSTFIELDSIZE, (long)RSTRING_LEN(body_str));
    CURL_SETOPT_CHECK(easy, CURLOPT_COPYPOSTFIELDS, StringValuePtr(body_str));
    RB_GC_GUARD(body_str);
    return CURLE_OK;
}

typedef struct {
    const char *name;
    const char *custom;
    int post;
    int nobody;
    int allows_body;
} http_method_t;

static const http_method_t HTTP_METHODS[] = {
    {"GET", NULL, 0, 0, 0},          {"POST", NULL, 1, 0, 1},     {"PUT", "PUT", 0, 0, 1},
    {"DELETE", "DELETE", 0, 0, 1},   {"PATCH", "PATCH", 0, 0, 1}, {"HEAD", NULL, 0, 1, 0},
    {"OPTIONS", "OPTIONS", 0, 0, 0},
};

static const http_method_t *find_http_method(const char *name) {
    for (size_t i = 0; i < sizeof(HTTP_METHODS) / sizeof(HTTP_METHODS[0]); i++) {
        if (strcasecmp(HTTP_METHODS[i].name, name) == 0)
            return &HTTP_METHODS[i];
    }
    return NULL;
}

static CURLcode apply_http_method(CURL *easy, const http_method_t *method) {
    if (method->post)
        CURL_SETOPT_CHECK(easy, CURLOPT_POST, 1L);
    if (method->custom)
        CURL_SETOPT_CHECK(easy, CURLOPT_CUSTOMREQUEST, method->custom);
    if (method->nobody)
        CURL_SETOPT_CHECK(easy, CURLOPT_NOBODY, 1L);
    return CURLE_OK;
}

static CURLcode setup_method_and_body(CURL *easy, VALUE method_value, VALUE body) {
    const char *name = NIL_P(method_value) ? "GET" : StringValueCStr(method_value);
    const http_method_t *method = find_http_method(name);
    int has_body = !NIL_P(body);

    if (!method)
        rb_raise(rb_eArgError, "Unsupported HTTP method: %s", name);
    if (has_body && !method->allows_body)
        rb_raise(rb_eArgError, "%s requests must not include a body", method->name);

    CURLcode res = apply_http_method(easy, method);
    if (res != CURLE_OK)
        return res;

    if (has_body) {
        res = set_body(easy, body);
        if (res != CURLE_OK)
            return res;
    }

    RB_GC_GUARD(method_value);
    RB_GC_GUARD(body);
    return CURLE_OK;
}

static void append_request_header(request_ctx_t *ctx, const char *buf) {
    struct curl_slist *new_headers = curl_slist_append(ctx->req_headers, buf);
    if (!new_headers)
        rb_raise(rb_eNoMemError, "failed to allocate request header");
    ctx->req_headers = new_headers;
}

static char *alloc_header_line(const char *key, long key_len, const char *value, long value_len,
                               char stack_buf[HEADER_LINE_BUF_SIZE]) {
    int has_value = value && value_len > 0;
    long need = key_len + (has_value ? 2 + value_len : 1) + 1;
    char *buf = need > HEADER_LINE_BUF_SIZE ? malloc((size_t)need) : stack_buf;

    if (!buf)
        rb_raise(rb_eNoMemError, "failed to allocate request header");

    memcpy(buf, key, (size_t)key_len);
    if (has_value) {
        buf[key_len] = ':';
        buf[key_len + 1] = ' ';
        memcpy(buf + key_len + 2, value, (size_t)value_len);
        buf[key_len + 2 + value_len] = '\0';
    } else {
        buf[key_len] = ';';
        buf[key_len + 1] = '\0';
    }

    return buf;
}

static void append_formatted_header(request_ctx_t *ctx, const char *key, long key_len,
                                    const char *value, long value_len) {
    char stack_buf[HEADER_LINE_BUF_SIZE];
    char *line = alloc_header_line(key, key_len, value, value_len, stack_buf);
    append_request_header(ctx, line);
    if (line != stack_buf)
        free(line);
}

static int header_iter_cb(VALUE key, VALUE val, VALUE arg) {
    request_ctx_t *ctx = (request_ctx_t *)arg;
    VALUE key_str = rb_String(key);
    VALUE val_str = NIL_P(val) ? Qnil : rb_String(val);
    const char *k = RSTRING_PTR(key_str);
    long klen = RSTRING_LEN(key_str);
    const char *v = NULL;
    long vlen = 0;

    if (!is_valid_header_name(k, klen) || contains_header_injection(k, klen))
        rb_raise(rb_eArgError, "Invalid HTTP header name");

    if (!NIL_P(val_str) && RSTRING_LEN(val_str) > 0) {
        v = RSTRING_PTR(val_str);
        vlen = RSTRING_LEN(val_str);
        if (contains_header_injection(v, vlen))
            rb_raise(rb_eArgError, "Invalid HTTP header value");
    }

    append_formatted_header(ctx, k, klen, v, vlen);

    RB_GC_GUARD(key_str);
    RB_GC_GUARD(val_str);
    return ST_CONTINUE;
}

static int setup_easy_handle(request_ctx_t *ctx, VALUE request, long timeout_sec) {
    Check_Type(request, T_HASH);

    VALUE url = hash_aref_key(request, KEY_URL);
    VALUE method = hash_aref_key(request, KEY_METHOD);
    VALUE headers = hash_aref_key(request, KEY_HEADERS);
    VALUE body = hash_aref_key(request, KEY_BODY);

    if (NIL_P(url))
        return 0;

    const char *url_str = StringValueCStr(url);
    if (!is_valid_url(url_str))
        rb_raise(rb_eArgError, "Invalid URL: %s", url_str);

    CURLcode res = setup_basic_options(ctx->easy, url_str, timeout_sec, ctx);
    if (res != CURLE_OK)
        return 0;

    res = setup_security_options(ctx->easy);
    if (res != CURLE_OK)
        return 0;

    res = setup_method_and_body(ctx->easy, method, body);
    if (res != CURLE_OK)
        return 0;

    if (!NIL_P(headers)) {
        Check_Type(headers, T_HASH);
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

static VALUE build_result_pair(int index, VALUE response) {
    return rb_ary_new_from_args(2, INT2NUM(index), response);
}

static int record_immediate_error(completion_ctx_t *cctx, int index, const char *message) {
    if (cctx->stream || cctx->target > 0) {
        VALUE pair = build_result_pair(index, build_error_response(message));

        if (cctx->stream)
            rb_yield(pair);
        else
            rb_ary_push(cctx->results, pair);

        cctx->completed++;
        if (cctx->target > 0 && cctx->completed >= cctx->target)
            return 1;
    }
    return 0;
}

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

        if (ctx->active) {
            curl_multi_remove_handle(session->multi, ctx->easy);
            ctx->active = 0;
            if (session->active_count > 0)
                session->active_count--;
        }

        ctx->done = 1;
        ctx->curl_result = msg->data.result;
        if (msg->data.result == CURLE_OK)
            curl_easy_getinfo(ctx->easy, CURLINFO_RESPONSE_CODE, &ctx->http_status);

        if (cctx->stream || cctx->target > 0) {
            VALUE response = (msg->data.result == CURLE_OK)
                                 ? build_response(ctx)
                                 : build_error_response_with_code(
                                       curl_easy_strerror(msg->data.result), (int)msg->data.result);
            VALUE pair = build_result_pair(ctx->index, response);

            if (cctx->stream)
                rb_yield(pair);
            else
                rb_ary_push(cctx->results, pair);
        }

        cctx->completed++;
        if (cctx->target > 0 && cctx->completed >= cctx->target)
            return 1;
    }

    return 0;
}

static int next_pending_index(multi_session_t *session) {
    if (session->pending_pos >= session->pending_count)
        return -1;

    if (session->pending_indices)
        return session->pending_indices[session->pending_pos++];

    return session->pending_pos++;
}

static int activate_request(multi_session_t *session, VALUE requests, int idx, int *invalid,
                            long timeout_sec) {
    request_ctx_t *ctx = &session->requests[idx];

    if (invalid[idx] || ctx->done)
        return 0;

    if (!request_ctx_prepare_easy(ctx))
        return 0;

    if (!setup_easy_handle(ctx, rb_ary_entry(requests, idx), timeout_sec))
        return 0;

    CURLMcode mc = curl_multi_add_handle(session->multi, ctx->easy);
    if (mc != CURLM_OK)
        return 0;

    ctx->active = 1;
    ctx->done = 0;
    session->active_count++;
    return 1;
}

static int fill_slots(multi_session_t *session, VALUE requests, int *invalid, long timeout_sec,
                      completion_ctx_t *cctx) {
    while (session->active_count < session->max_connections) {
        int idx = next_pending_index(session);
        if (idx < 0)
            break;

        request_ctx_t *ctx = &session->requests[idx];

        if (!activate_request(session, requests, idx, invalid, timeout_sec)) {
            invalid[idx] = 1;
            ctx->done = 1;
            ctx->active = 0;
            if (record_immediate_error(cctx, idx, "Invalid request configuration"))
                return 1;
        }
    }

    return 0;
}

static int pending_remaining(multi_session_t *session) {
    return session->pending_pos < session->pending_count;
}

static void prepare_pending(multi_session_t *session, int *indices, int count) {
    session->pending_indices = indices;
    session->pending_count = count;
    session->pending_pos = 0;
    session->active_count = 0;
    session->still_running = 0;
}

static void run_multi_loop(multi_session_t *session, completion_ctx_t *cctx, VALUE requests,
                           int *invalid, long timeout_sec, int *indices, int indices_count) {
#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
    VALUE scheduler = current_fiber_scheduler();
#endif
    prepare_pending(session, indices, indices_count);

    if (fill_slots(session, requests, invalid, timeout_sec, cctx))
        return;

    curl_multi_perform(session->multi, &session->still_running);
    if (process_completed(session, cctx))
        return;

    while (!session->cancelled && (session->active_count > 0 || pending_remaining(session))) {
        if (fill_slots(session, requests, invalid, timeout_sec, cctx))
            return;

        if (session->active_count == 0)
            break;

#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
        if (scheduler != Qnil)
            run_via_fiber_worker(scheduler, poll_without_gvl, session);
        else
#endif
            rb_thread_call_without_gvl(poll_without_gvl, session, unblock_perform, session);

        if (process_completed(session, cctx))
            return;
    }

    process_completed(session, cctx);
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

static void retry_delay_sleep(long delay_ms) {
    if (delay_ms <= 0)
        return;

#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
    VALUE scheduler = current_fiber_scheduler();
    if (scheduler != Qnil) {
        long remaining = delay_ms;
        while (remaining > 0) {
            long chunk = remaining > FIBER_POLL_TIMEOUT_MS ? FIBER_POLL_TIMEOUT_MS : remaining;
            sleep_arg_t sa = {.delay_ms = chunk};
            run_via_fiber_worker(scheduler, sleep_without_gvl, &sa);
            remaining -= chunk;
        }
    } else
#endif
    {
        sleep_arg_t sa = {.delay_ms = delay_ms};
        rb_thread_call_without_gvl(sleep_without_gvl, &sa, (rb_unblock_function_t *)0, NULL);
    }
}

static void retry_config_init(retry_config_t *retry_cfg) {
    retry_cfg->max_retries = DEFAULT_RETRIES;
    retry_cfg->retries_explicit = 0;
    retry_cfg->retry_delay_ms = DEFAULT_RETRY_DELAY;
    retry_cfg->retry_http_codes = NULL;
    retry_cfg->retry_http_count = 0;
}

static long parse_long_option(VALUE options, key_id_t key, const char *name, long min, long max,
                              long default_value, int *present) {
    VALUE raw = hash_aref_key(options, key);
    long value;

    if (present)
        *present = !NIL_P(raw);
    if (NIL_P(raw))
        return default_value;

    value = NUM2LONG(raw);
    if (value < min || value > max)
        rb_raise(rb_eArgError, "%s must be between %ld and %ld", name, min, max);

    return value;
}

static int parse_int_option(VALUE options, key_id_t key, const char *name, int min, int max,
                            int default_value, int *present) {
    return (int)parse_long_option(options, key, name, min, max, default_value, present);
}

static void parse_retry_codes(VALUE options, retry_config_t *retry_cfg) {
    VALUE codes = hash_aref_key(options, KEY_RETRY_CODES);
    long len_long;
    int len;

    if (NIL_P(codes))
        return;

    Check_Type(codes, T_ARRAY);
    len_long = RARRAY_LEN(codes);
    if (len_long > INT_MAX)
        rb_raise(rb_eArgError, "retry_codes is too large");

    len = (int)len_long;
    for (int i = 0; i < len; i++) {
        int code = NUM2INT(rb_ary_entry(codes, i));
        if (code < 100 || code > 599)
            rb_raise(rb_eArgError, "retry_codes must contain valid HTTP status codes");
    }

    if (len == 0)
        return;

    retry_cfg->retry_http_codes = malloc(sizeof(int) * (size_t)len);
    if (!retry_cfg->retry_http_codes)
        rb_raise(rb_eNoMemError, "failed to allocate retry codes");

    retry_cfg->retry_http_count = len;
    for (int i = 0; i < len; i++)
        retry_cfg->retry_http_codes[i] = NUM2INT(rb_ary_entry(codes, i));
}

static void parse_options(VALUE options, long *timeout, int *max_conn, retry_config_t *retry_cfg) {
    *timeout = 30;
    *max_conn = 20;
    retry_config_init(retry_cfg);

    if (NIL_P(options))
        return;

    Check_Type(options, T_HASH);
    *timeout = parse_long_option(options, KEY_TIMEOUT, "timeout", 1, MAX_TIMEOUT, *timeout, NULL);
    *max_conn = parse_int_option(options, KEY_CONNECTIONS, "connections", 1, MAX_CONNECTIONS,
                                 *max_conn, NULL);
    retry_cfg->max_retries = parse_int_option(options, KEY_RETRIES, "retries", 0, MAX_RETRIES,
                                              retry_cfg->max_retries, &retry_cfg->retries_explicit);
    retry_cfg->retry_delay_ms =
        parse_long_option(options, KEY_RETRY_DELAY, "retry_delay", 0, MAX_RETRY_DELAY_MS,
                          retry_cfg->retry_delay_ms, NULL);
    parse_retry_codes(options, retry_cfg);
}

static void multi_session_init(multi_session_t *session, CURLM *multi, int count, int max_conn,
                               long timeout_sec) {
    memset(session, 0, sizeof(*session));
    session->multi = multi;
    session->count = count;
    session->timeout_ms = timeout_sec * 1000;
    session->max_connections = max_conn;
}

static void multi_session_configure(CURLM *multi, int max_conn) {
    curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS, (long)max_conn);
    curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, (long)max_conn);
#ifdef CURLPIPE_MULTIPLEX
    curl_multi_setopt(multi, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif
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
            if (ctx->session->requests[i].easy && ctx->session->requests[i].active)
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

static FAST_CURL_NORETURN void cleanup_and_raise(cleanup_ctx_t *cleanup, VALUE exception,
                                                 const char *message) {
    cleanup_session((VALUE)cleanup);
    rb_raise(exception, "%s", message);
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
    int count = session->count;
    int target = ea->target;
    int stream = ea->stream;

    for (int i = 0; i < count; i++)
        request_ctx_init(&session->requests[i], i);

    completion_ctx_t cctx;
    cctx.results = stream ? Qnil : ((target > 0) ? rb_ary_new2(target) : rb_ary_new2(count));
    cctx.completed = 0;
    cctx.target = target;
    cctx.stream = stream;

    if (!stream && target <= 0) {
        for (int i = 0; i < count; i++)
            rb_ary_store(cctx.results, i, Qnil);
    }

    run_multi_loop(session, &cctx, requests, invalid, timeout_sec, NULL, count);

    if (!stream && target <= 0 && retry_cfg->max_retries > 0) {
        for (int attempt = 0; attempt < retry_cfg->max_retries; attempt++) {
            int *retry_indices = malloc(sizeof(int) * (size_t)count);
            if (!retry_indices)
                rb_raise(rb_eNoMemError, "failed to allocate retry index array");

            int retry_count = 0;
            for (int i = 0; i < count; i++) {
                if (invalid[i] || !session->requests[i].done)
                    continue;
                if (should_retry(&session->requests[i], retry_cfg))
                    retry_indices[retry_count++] = i;
            }

            if (retry_count == 0) {
                free(retry_indices);
                break;
            }

            retry_delay_sleep(retry_cfg->retry_delay_ms);

            int runnable_count = 0;
            for (int r = 0; r < retry_count; r++) {
                int idx = retry_indices[r];
                request_ctx_t *rc = &session->requests[idx];

                if (!request_ctx_reset_for_retry(rc)) {
                    invalid[idx] = 1;
                    rc->done = 1;
                    continue;
                }

                retry_indices[runnable_count++] = idx;
            }

            if (runnable_count > 0) {
                cctx.completed = 0;
                run_multi_loop(session, &cctx, requests, invalid, timeout_sec, retry_indices,
                               runnable_count);
            }

            free(retry_indices);
        }
    }

    if (!stream && target <= 0) {
        for (int i = 0; i < count; i++) {
            request_ctx_t *rc = &session->requests[i];
            VALUE response;

            if (invalid[i]) {
                response = build_error_response("Invalid request configuration");
            } else if (!rc->done) {
                response = build_error_response("Request was not completed");
            } else if (rc->curl_result == CURLE_OK) {
                response = build_response(rc);
            } else {
                response = build_error_response_with_code(curl_easy_strerror(rc->curl_result),
                                                          (int)rc->curl_result);
            }

            rb_ary_store(cctx.results, i, build_result_pair(i, response));
        }
    }

    return stream ? Qnil : cctx.results;
}

#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
typedef struct {
    execute_args_t *ea;
    VALUE scheduler;
    VALUE blocker;
    VALUE fiber;
    VALUE thread;
    VALUE result;
    VALUE exception;
    int state;
    int finished;
} scheduler_execute_ctx_t;

static VALUE scheduler_execute_thread(void *arg) {
    scheduler_execute_ctx_t *ctx = (scheduler_execute_ctx_t *)arg;

    ctx->result = rb_protect(internal_execute_body, (VALUE)ctx->ea, &ctx->state);
    if (ctx->state) {
        ctx->exception = rb_errinfo();
        rb_set_errinfo(Qnil);
    }

    ctx->finished = 1;
    rb_fiber_scheduler_unblock(ctx->scheduler, ctx->blocker, ctx->fiber);
    return Qnil;
}

static void cancel_scheduler_execute(scheduler_execute_ctx_t *ctx) {
    multi_session_t *session = ctx->ea->session;

    session->cancelled = 1;
#ifdef HAVE_CURL_MULTI_WAKEUP
    if (session->multi)
        curl_multi_wakeup(session->multi);
#endif
}

static VALUE scheduler_execute_wait(VALUE arg) {
    scheduler_execute_ctx_t *ctx = (scheduler_execute_ctx_t *)arg;

    if (!ctx->finished)
        rb_fiber_scheduler_block(ctx->scheduler, ctx->blocker, Qnil);

    rb_funcall(ctx->thread, rb_intern("join"), 0);

    if (ctx->state)
        rb_exc_raise(ctx->exception);

    return ctx->result;
}

static VALUE scheduler_execute_ensure(VALUE arg) {
    scheduler_execute_ctx_t *ctx = (scheduler_execute_ctx_t *)arg;

    if (!NIL_P(ctx->thread)) {
        if (!ctx->finished)
            cancel_scheduler_execute(ctx);
        rb_funcall(ctx->thread, rb_intern("join"), 0);
    }

    return Qnil;
}

static VALUE execute_with_fiber_scheduler(VALUE arg) {
    scheduler_execute_ctx_t *ctx = (scheduler_execute_ctx_t *)arg;

    ctx->blocker = rb_obj_alloc(rb_cObject);
    ctx->fiber = rb_fiber_current();
    ctx->thread = rb_thread_create(scheduler_execute_thread, ctx);

    return rb_ensure(scheduler_execute_wait, arg, scheduler_execute_ensure, arg);
}
#endif

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

    if (target > 0 && target > count)
        target = count;

    long timeout_sec;
    int max_conn;
    retry_config_t retry_cfg;
    parse_options(options, &timeout_sec, &max_conn, &retry_cfg);

    if (stream || target > 0) {
        if (retry_cfg.retries_explicit && retry_cfg.max_retries > 0 && stream)
            rb_warn(
                "FastCurl: retries are not supported in stream_execute, ignoring retries option");
        if (retry_cfg.retries_explicit && retry_cfg.max_retries > 0 && target > 0)
            rb_warn(
                "FastCurl: retries are not supported in first_execute, ignoring retries option");
        retry_cfg.max_retries = 0;
    }

    multi_session_t session;
    int *invalid = NULL;
    multi_session_init(&session, curl_multi_init(), count, max_conn, timeout_sec);

    cleanup_ctx_t cleanup = {.session = &session, .invalid = NULL, .retry_cfg = &retry_cfg};

    if (!session.multi)
        cleanup_and_raise(&cleanup, rb_eNoMemError, "failed to initialize curl multi handle");

    multi_session_configure(session.multi, max_conn);

    session.requests = calloc((size_t)count, sizeof(request_ctx_t));
    if (!session.requests)
        cleanup_and_raise(&cleanup, rb_eNoMemError, "failed to allocate request contexts");

    invalid = calloc((size_t)count, sizeof(int));
    cleanup.invalid = invalid;
    if (!invalid)
        cleanup_and_raise(&cleanup, rb_eNoMemError, "failed to allocate tracking array");
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

#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
    VALUE scheduler = current_fiber_scheduler();
    if (scheduler != Qnil && !stream) {
        scheduler_execute_ctx_t scheduler_ctx = {
            .ea = &ea,
            .scheduler = scheduler,
            .blocker = Qnil,
            .fiber = Qnil,
            .thread = Qnil,
            .result = Qnil,
            .exception = Qnil,
            .state = 0,
            .finished = 0,
        };

        return rb_ensure(execute_with_fiber_scheduler, (VALUE)&scheduler_ctx, cleanup_session,
                         (VALUE)&cleanup);
    }
#endif

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
        Check_Type(options, T_HASH);
        VALUE c = hash_aref_key(options, KEY_COUNT_OPT);
        if (!NIL_P(c))
            count = NUM2INT(c);
    }

    if (count <= 0)
        rb_raise(rb_eArgError, "count must be positive");

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

    for (int i = 0; i < KEY_LAST; i++) {
        fast_ids[i] = rb_intern(KEY_NAMES[i]);
        fast_syms[i] = ID2SYM(fast_ids[i]);
        rb_gc_register_address(&fast_syms[i]);
    }

    VALUE mFastCurl = rb_define_module("FastCurl");

    rb_define_module_function(mFastCurl, "execute", rb_fast_curl_execute, -1);
    rb_define_module_function(mFastCurl, "first_execute", rb_fast_curl_first_execute, -1);
    rb_define_module_function(mFastCurl, "stream_execute", rb_fast_curl_stream_execute, -1);
}
