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
#include <ctype.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#if defined(__GNUC__) || defined(__clang__)
#define FAST_CURL_NORETURN __attribute__((noreturn))
#else
#define FAST_CURL_NORETURN
#endif

#define MAX_RESPONSE_SIZE          (100 * 1024 * 1024)
#define MAX_REDIRECTS              5
#define MAX_TIMEOUT                300
#define MAX_RETRIES                10
#define MAX_REQUESTS               10000
#define MAX_CONNECTIONS            100
#define MAX_RETRY_DELAY_MS         30000
#define DEFAULT_RETRIES            1
#define DEFAULT_RETRY_DELAY        100
#define DEFAULT_CONNECT_TIMEOUT_MS 10000L
#define MAX_CONNECT_TIMEOUT_MS     300000L
#define MAX_TOTAL_TIMEOUT_MS       3600000L
#define INITIAL_BUF_CAP            8192
#define INITIAL_HEADER_CAP         16
#define POLL_TIMEOUT_MS            50
#define POLL_SLICE_MS              2000
#define FIBER_POLL_TIMEOUT_MS      10
#define HEADER_LINE_BUF_SIZE       512

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
    KEY_CONNECT_TIMEOUT,
    KEY_TOTAL_TIMEOUT,
    KEY_RETRY_NON_IDEMPOTENT,
    KEY_FOLLOW_REDIRECTS,
    KEY_MAX_REDIRECTS,
    KEY_EFFECTIVE_URL,
    KEY_ERROR,
    KEY_ATTEMPTS,
    KEY_LAST
} key_id_t;

static ID fast_ids[KEY_LAST];
static VALUE fast_syms[KEY_LAST];

#define SYM(key) fast_syms[key]

static const char *const KEY_NAMES[KEY_LAST] = {
    "status",
    "headers",
    "body",
    "error_code",
    "url",
    "method",
    "timeout",
    "connections",
    "count",
    "retries",
    "retry_delay",
    "retry_codes",
    "connect_timeout",
    "total_timeout",
    "retry_non_idempotent",
    "follow_redirects",
    "max_redirects",
    "effective_url",
    "error",
    "attempts",
};

typedef enum {
    ERR_CURL,
    ERR_INVALID_REQUEST,
    ERR_NOT_COMPLETED,
    ERR_DEADLINE,
    ERR_LAST
} error_kind_t;

static VALUE error_syms[ERR_LAST];

static const char *const ERROR_KIND_NAMES[ERR_LAST] = {
    "curl_error",
    "invalid_request",
    "not_completed",
    "deadline_exceeded",
};

static long long fast_now_ms(void) {
    struct timespec ts;
#ifdef CLOCK_MONOTONIC
    clock_gettime(CLOCK_MONOTONIC, &ts);
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static uint64_t fast_rand_state;

/* Small xorshift PRNG; only used to jitter retry delays. */
static uint32_t fast_rand(void) {
    uint64_t x = fast_rand_state;
    if (x == 0)
        x = (uint64_t)fast_now_ms() | 1ULL;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    fast_rand_state = x;
    return (uint32_t)(x >> 32);
}

/* Process-wide DNS and TLS session cache, so repeated calls to the same host
   skip resolution and can resume TLS instead of doing a full handshake. */
static CURLSH *fast_share = NULL;
static pthread_mutex_t fast_share_locks[CURL_LOCK_DATA_LAST];

static void fast_share_lock(CURL *handle, curl_lock_data data, curl_lock_access access,
                            void *userptr) {
    (void)handle;
    (void)access;
    (void)userptr;
    if ((int)data > 0 && (int)data < CURL_LOCK_DATA_LAST)
        pthread_mutex_lock(&fast_share_locks[data]);
}

static void fast_share_unlock(CURL *handle, curl_lock_data data, void *userptr) {
    (void)handle;
    (void)userptr;
    if ((int)data > 0 && (int)data < CURL_LOCK_DATA_LAST)
        pthread_mutex_unlock(&fast_share_locks[data]);
}

static void fast_share_init(void) {
    for (int i = 0; i < CURL_LOCK_DATA_LAST; i++)
        pthread_mutex_init(&fast_share_locks[i], NULL);

    fast_share = curl_share_init();
    if (!fast_share)
        return;

    curl_share_setopt(fast_share, CURLSHOPT_LOCKFUNC, fast_share_lock);
    curl_share_setopt(fast_share, CURLSHOPT_UNLOCKFUNC, fast_share_unlock);
    curl_share_setopt(fast_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(fast_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);

    /* CURL_LOCK_DATA_CONNECT is deliberately NOT shared. Sharing the connection
       cache between multi handles running concurrently in different threads
       deadlocks against CURLMOPT_MAX_TOTAL_CONNECTIONS, and removing that limit
       turns the deadlock into a segfault inside curl_multi_perform (reproduced
       on libcurl 8.5.0 with two threads x 25 requests). Cross-call TCP reuse
       needs a persistent multi handle per thread, not a shared cache. */
}

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
    int idempotent;
    int attempts;
    const char *setup_error;
    int setup_error_fatal;
} request_ctx_t;

static VALUE fast_validation_error = Qnil;
#define SET_SETUP_ERROR(ctx, msg, fatal)    \
    do {                                    \
        (ctx)->setup_error = (msg);         \
        (ctx)->setup_error_fatal = (fatal); \
    } while (0)

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
    ctx->idempotent = 1;
    ctx->attempts = 0;
    ctx->setup_error = NULL;
    ctx->setup_error_fatal = 0;
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
    ctx->setup_error = NULL;
    ctx->setup_error_fatal = 0;
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
    int retry_non_idempotent;
} retry_config_t;

typedef struct {
    long timeout_sec;
    long connect_timeout_ms;
    long total_timeout_ms;
    long follow_redirects;
    long max_redirects;
    long long deadline_ms;
} request_options_t;

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

static VALUE fast_cHeaders = Qnil;
static VALUE new_headers_hash(void) {
    if (!NIL_P(fast_cHeaders))
        return rb_obj_alloc(fast_cHeaders);
    return rb_hash_new();
}

static void headers_hash_store(VALUE headers_hash, VALUE key, VALUE val, int always_array) {
    VALUE existing = rb_hash_aref(headers_hash, key);

    if (NIL_P(existing)) {
        rb_hash_aset(headers_hash, key, always_array ? rb_ary_new_from_args(1, val) : val);
        return;
    }

    if (RB_TYPE_P(existing, T_ARRAY)) {
        rb_ary_push(existing, val);
        return;
    }

    VALUE joined = rb_str_dup(existing);
    rb_str_cat_cstr(joined, ", ");
    rb_str_append(joined, val);
    rb_hash_aset(headers_hash, key, joined);
}

static VALUE build_response(request_ctx_t *ctx) {
    long status = 0;
    curl_easy_getinfo(ctx->easy, CURLINFO_RESPONSE_CODE, &status);

    VALUE headers_hash = new_headers_hash();
    for (int i = 0; i < ctx->headers.count; i++) {
        const char *hdr = ctx->headers.entries[i].str;
        size_t hdr_len = ctx->headers.entries[i].len;
        const char *colon = memchr(hdr, ':', hdr_len);
        if (!colon)
            continue;

        VALUE key = rb_str_new(hdr, colon - hdr);
        char *kp = RSTRING_PTR(key);
        long klen = RSTRING_LEN(key);
        for (long k = 0; k < klen; k++)
            kp[k] = (char)tolower((unsigned char)kp[k]);
        int always_array = (klen == 10 && memcmp(kp, "set-cookie", 10) == 0);

        const char *vs = colon + 1;
        const char *ve = hdr + hdr_len;

        while (vs < ve && (*vs == ' ' || *vs == '\t'))
            vs++;
        while (ve > vs && (*(ve - 1) == ' ' || *(ve - 1) == '\t'))
            ve--;

        VALUE val = rb_str_new(vs, ve - vs);
        headers_hash_store(headers_hash, key, val, always_array);
    }

    VALUE body_str =
        ctx->body.data ? rb_str_new(ctx->body.data, ctx->body.len) : rb_str_new_cstr("");

    VALUE result = rb_hash_new();
    rb_hash_aset(result, SYM(KEY_STATUS), LONG2NUM(status));
    rb_hash_aset(result, SYM(KEY_HEADERS), headers_hash);
    rb_hash_aset(result, SYM(KEY_BODY), body_str);
    rb_hash_aset(result, SYM(KEY_ERROR), Qnil);
    rb_hash_aset(result, SYM(KEY_ERROR_CODE), Qnil);
    rb_hash_aset(result, SYM(KEY_ATTEMPTS), INT2NUM(ctx->attempts));

    char *effective = NULL;
    if (curl_easy_getinfo(ctx->easy, CURLINFO_EFFECTIVE_URL, &effective) == CURLE_OK && effective)
        rb_hash_aset(result, SYM(KEY_EFFECTIVE_URL), rb_str_new_cstr(effective));
    else
        rb_hash_aset(result, SYM(KEY_EFFECTIVE_URL), Qnil);

    return result;
}

static VALUE build_error_response(const char *message, error_kind_t kind, int attempts) {
    VALUE r = rb_hash_new();
    rb_hash_aset(r, SYM(KEY_STATUS), INT2NUM(0));
    rb_hash_aset(r, SYM(KEY_HEADERS), Qnil);
    rb_hash_aset(r, SYM(KEY_BODY), rb_str_new_cstr(message));
    rb_hash_aset(r, SYM(KEY_ERROR), error_syms[kind]);
    rb_hash_aset(r, SYM(KEY_ERROR_CODE), Qnil);
    rb_hash_aset(r, SYM(KEY_EFFECTIVE_URL), Qnil);
    rb_hash_aset(r, SYM(KEY_ATTEMPTS), INT2NUM(attempts));
    return r;
}

static VALUE build_error_response_with_code(const char *message, int error_code, int attempts) {
    VALUE r = build_error_response(message, ERR_CURL, attempts);
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

static CURLcode setup_basic_options(CURL *easy, const char *url_str, const request_options_t *opts,
                                    request_ctx_t *ctx) {
    long timeout_ms = opts->timeout_sec * 1000L;

    if (opts->deadline_ms > 0) {
        long long remaining = opts->deadline_ms - fast_now_ms();
        if (remaining < 1)
            remaining = 1;
        if (remaining < (long long)timeout_ms)
            timeout_ms = (long)remaining;
    }

    CURL_SETOPT_CHECK(easy, CURLOPT_URL, url_str);
    CURL_SETOPT_CHECK(easy, CURLOPT_WRITEFUNCTION, write_callback);
    CURL_SETOPT_CHECK(easy, CURLOPT_WRITEDATA, &ctx->body);
    CURL_SETOPT_CHECK(easy, CURLOPT_HEADERFUNCTION, header_callback);
    CURL_SETOPT_CHECK(easy, CURLOPT_HEADERDATA, &ctx->headers);
    CURL_SETOPT_CHECK(easy, CURLOPT_TIMEOUT_MS, timeout_ms);
    CURL_SETOPT_CHECK(easy, CURLOPT_CONNECTTIMEOUT_MS, opts->connect_timeout_ms);
    CURL_SETOPT_CHECK(easy, CURLOPT_NOSIGNAL, 1L);
    CURL_SETOPT_CHECK(easy, CURLOPT_FOLLOWLOCATION, opts->follow_redirects);
    CURL_SETOPT_CHECK(easy, CURLOPT_MAXREDIRS, opts->max_redirects);
    CURL_SETOPT_CHECK(easy, CURLOPT_ACCEPT_ENCODING, "");
    CURL_SETOPT_CHECK(easy, CURLOPT_PRIVATE, (char *)ctx);
    CURL_SETOPT_CHECK(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    if (fast_share)
        CURL_SETOPT_CHECK(easy, CURLOPT_SHARE, fast_share);
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
    int idempotent;
} http_method_t;

static const http_method_t HTTP_METHODS[] = {
    {"GET", NULL, 0, 0, 0, 1},          {"POST", NULL, 1, 0, 1, 0},     {"PUT", "PUT", 0, 0, 1, 1},
    {"DELETE", "DELETE", 0, 0, 1, 1},   {"PATCH", "PATCH", 0, 0, 1, 0}, {"HEAD", NULL, 0, 1, 0, 1},
    {"OPTIONS", "OPTIONS", 0, 0, 0, 1},
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

static int setup_method_and_body(request_ctx_t *ctx, VALUE method_value, VALUE body) {
    const http_method_t *method;
    int has_body = !NIL_P(body);
    char name[32];

    if (NIL_P(method_value)) {
        memcpy(name, "GET", 4);
    } else {
        if (!RB_TYPE_P(method_value, T_STRING)) {
            SET_SETUP_ERROR(ctx, "Unsupported HTTP method", 1);
            return 0;
        }
        long len = RSTRING_LEN(method_value);
        if (len <= 0 || len >= (long)sizeof(name)) {
            SET_SETUP_ERROR(ctx, "Unsupported HTTP method", 1);
            return 0;
        }
        memcpy(name, RSTRING_PTR(method_value), (size_t)len);
        name[len] = '\0';
    }

    method = find_http_method(name);
    if (!method) {
        SET_SETUP_ERROR(ctx, "Unsupported HTTP method", 1);
        return 0;
    }
    if (has_body && !method->allows_body) {
        SET_SETUP_ERROR(ctx, "This HTTP method must not include a body", 1);
        return 0;
    }

    ctx->idempotent = method->idempotent;

    if (apply_http_method(ctx->easy, method) != CURLE_OK) {
        SET_SETUP_ERROR(ctx, "Failed to configure HTTP method", 0);
        return 0;
    }

    if (has_body && set_body(ctx->easy, body) != CURLE_OK) {
        SET_SETUP_ERROR(ctx, "Failed to configure request body", 0);
        return 0;
    }

    RB_GC_GUARD(method_value);
    RB_GC_GUARD(body);
    return 1;
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

static VALUE header_part_to_str(VALUE v) {
    if (RB_TYPE_P(v, T_STRING))
        return v;
    if (SYMBOL_P(v))
        return rb_sym2str(v);
    return rb_String(v);
}

static int header_iter_cb(VALUE key, VALUE val, VALUE arg) {
    request_ctx_t *ctx = (request_ctx_t *)arg;
    VALUE key_str, val_str;
    const char *k;
    long klen;
    const char *v = NULL;
    long vlen = 0;

    if (ctx->setup_error)
        return ST_STOP;

    key_str = header_part_to_str(key);
    val_str = NIL_P(val) ? Qnil : header_part_to_str(val);
    k = RSTRING_PTR(key_str);
    klen = RSTRING_LEN(key_str);

    if (!is_valid_header_name(k, klen) || contains_header_injection(k, klen)) {
        SET_SETUP_ERROR(ctx, "Invalid HTTP header name", 1);
        return ST_STOP;
    }

    if (!NIL_P(val_str) && RSTRING_LEN(val_str) > 0) {
        v = RSTRING_PTR(val_str);
        vlen = RSTRING_LEN(val_str);
        if (contains_header_injection(v, vlen)) {
            SET_SETUP_ERROR(ctx, "Invalid HTTP header value", 1);
            return ST_STOP;
        }
    }

    append_formatted_header(ctx, k, klen, v, vlen);

    RB_GC_GUARD(key_str);
    RB_GC_GUARD(val_str);
    return ST_CONTINUE;
}

static int setup_easy_handle(request_ctx_t *ctx, VALUE request, const request_options_t *opts) {
    if (!RB_TYPE_P(request, T_HASH)) {
        SET_SETUP_ERROR(ctx, "Request must be a Hash", 1);
        return 0;
    }

    VALUE url = hash_aref_key(request, KEY_URL);
    VALUE method = hash_aref_key(request, KEY_METHOD);
    VALUE headers = hash_aref_key(request, KEY_HEADERS);
    VALUE body = hash_aref_key(request, KEY_BODY);

    if (NIL_P(url)) {
        SET_SETUP_ERROR(ctx, "Missing :url", 0);
        return 0;
    }
    if (!RB_TYPE_P(url, T_STRING) || memchr(RSTRING_PTR(url), '\0', RSTRING_LEN(url))) {
        SET_SETUP_ERROR(ctx, "Invalid URL", 1);
        return 0;
    }

    const char *url_str = StringValueCStr(url);
    if (!is_valid_url(url_str)) {
        SET_SETUP_ERROR(ctx, "Invalid URL (expected http:// or https://, max 2048 bytes)", 1);
        return 0;
    }

    if (setup_basic_options(ctx->easy, url_str, opts, ctx) != CURLE_OK) {
        SET_SETUP_ERROR(ctx, "Failed to configure request", 0);
        return 0;
    }

    if (setup_security_options(ctx->easy) != CURLE_OK) {
        SET_SETUP_ERROR(ctx, "Failed to configure TLS options", 0);
        return 0;
    }

    if (!setup_method_and_body(ctx, method, body))
        return 0;

    if (!NIL_P(headers)) {
        if (!RB_TYPE_P(headers, T_HASH)) {
            SET_SETUP_ERROR(ctx, ":headers must be a Hash", 1);
            return 0;
        }
        rb_hash_foreach(headers, header_iter_cb, (VALUE)ctx);
        if (ctx->setup_error)
            return 0;
        if (ctx->req_headers) {
            if (curl_easy_setopt(ctx->easy, CURLOPT_HTTPHEADER, ctx->req_headers) != CURLE_OK) {
                SET_SETUP_ERROR(ctx, "Failed to set request headers", 0);
                return 0;
            }
        }
    }

    RB_GC_GUARD(url);
    RB_GC_GUARD(method);
    RB_GC_GUARD(headers);
    RB_GC_GUARD(body);
    RB_GC_GUARD(request);
    return 1;
}

/* Poll until something actually completes, or the slice budget expires.
   Previously this returned after every single 50ms poll, which under a fiber
   scheduler meant a fresh OS thread twenty times a second. Cancellation still
   breaks out immediately via curl_multi_wakeup. */
static void *poll_without_gvl(void *arg) {
    multi_session_t *s = (multi_session_t *)arg;
    long long started = fast_now_ms();
    int before = s->still_running;

    while (!s->cancelled) {
        int numfds = 0;
        curl_multi_poll(s->multi, NULL, 0, POLL_TIMEOUT_MS, &numfds);
        curl_multi_perform(s->multi, &s->still_running);

        if (s->still_running == 0 || s->still_running < before)
            break;
        if (fast_now_ms() - started >= POLL_SLICE_MS)
            break;
    }
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

static int record_immediate_error(completion_ctx_t *cctx, int index, const char *message,
                                  int attempts) {
    if (cctx->stream || cctx->target > 0) {
        VALUE pair =
            build_result_pair(index, build_error_response(message, ERR_INVALID_REQUEST, attempts));

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
            VALUE response =
                (msg->data.result == CURLE_OK)
                    ? build_response(ctx)
                    : build_error_response_with_code(curl_easy_strerror(msg->data.result),
                                                     (int)msg->data.result, ctx->attempts);
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
                            const request_options_t *opts) {
    request_ctx_t *ctx = &session->requests[idx];

    if (invalid[idx] || ctx->done)
        return 0;

    if (!request_ctx_prepare_easy(ctx)) {
        SET_SETUP_ERROR(ctx, "Failed to allocate a curl handle", 0);
        return 0;
    }

    if (!setup_easy_handle(ctx, rb_ary_entry(requests, idx), opts))
        return 0;

    if (curl_multi_add_handle(session->multi, ctx->easy) != CURLM_OK) {
        SET_SETUP_ERROR(ctx, "Failed to schedule the request", 0);
        return 0;
    }

    ctx->attempts++;
    ctx->active = 1;
    ctx->done = 0;
    session->active_count++;
    return 1;
}

static int fill_slots(multi_session_t *session, VALUE requests, int *invalid,
                      const request_options_t *opts, completion_ctx_t *cctx) {
    while (session->active_count < session->max_connections) {
        int idx = next_pending_index(session);
        if (idx < 0)
            break;

        request_ctx_t *ctx = &session->requests[idx];

        if (!activate_request(session, requests, idx, invalid, opts)) {
            invalid[idx] = 1;
            ctx->done = 1;
            ctx->active = 0;
            if (record_immediate_error(cctx, idx,
                                       ctx->setup_error ? ctx->setup_error
                                                        : "Invalid request configuration",
                                       ctx->attempts))
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
                           int *invalid, const request_options_t *opts, int *indices,
                           int indices_count) {
#ifdef FAST_CURL_HAVE_FIBER_SCHEDULER
    VALUE scheduler = current_fiber_scheduler();
#endif
    prepare_pending(session, indices, indices_count);

    if (fill_slots(session, requests, invalid, opts, cctx))
        return;

    curl_multi_perform(session->multi, &session->still_running);
    if (process_completed(session, cctx))
        return;

    while (!session->cancelled && (session->active_count > 0 || pending_remaining(session))) {
        if (opts->deadline_ms > 0 && fast_now_ms() >= opts->deadline_ms)
            break;

        if (fill_slots(session, requests, invalid, opts, cctx))
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
    if (!ctx->idempotent && !rc->retry_non_idempotent)
        return 0;

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
    retry_cfg->retry_non_idempotent = 0;
}

static long retry_backoff_ms(const retry_config_t *rc, int attempt) {
    long delay = rc->retry_delay_ms;
    long half;

    if (delay <= 0)
        return 0;

    for (int i = 0; i < attempt && delay < MAX_RETRY_DELAY_MS; i++)
        delay *= 2;
    if (delay > MAX_RETRY_DELAY_MS)
        delay = MAX_RETRY_DELAY_MS;

    half = delay / 2;
    if (half > 0)
        delay = half + (long)(fast_rand() % (uint32_t)(half + 1));

    return delay;
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

static void parse_options(VALUE options, request_options_t *opts, int *max_conn,
                          retry_config_t *retry_cfg) {
    VALUE flag;

    opts->timeout_sec = 30;
    opts->connect_timeout_ms = DEFAULT_CONNECT_TIMEOUT_MS;
    opts->total_timeout_ms = 0;
    opts->follow_redirects = 1;
    opts->max_redirects = MAX_REDIRECTS;
    opts->deadline_ms = 0;
    *max_conn = 20;
    retry_config_init(retry_cfg);

    if (NIL_P(options))
        return;

    Check_Type(options, T_HASH);
    opts->timeout_sec =
        parse_long_option(options, KEY_TIMEOUT, "timeout", 1, MAX_TIMEOUT, opts->timeout_sec, NULL);
    opts->connect_timeout_ms =
        parse_long_option(options, KEY_CONNECT_TIMEOUT, "connect_timeout", 1,
                          MAX_CONNECT_TIMEOUT_MS, opts->connect_timeout_ms, NULL);
    opts->total_timeout_ms = parse_long_option(options, KEY_TOTAL_TIMEOUT, "total_timeout", 1,
                                               MAX_TOTAL_TIMEOUT_MS, opts->total_timeout_ms, NULL);
    opts->max_redirects = parse_long_option(options, KEY_MAX_REDIRECTS, "max_redirects", 0, 100,
                                            opts->max_redirects, NULL);

    flag = hash_aref_key(options, KEY_FOLLOW_REDIRECTS);
    if (!NIL_P(flag))
        opts->follow_redirects = RTEST(flag) ? 1L : 0L;

    flag = hash_aref_key(options, KEY_RETRY_NON_IDEMPOTENT);
    if (!NIL_P(flag))
        retry_cfg->retry_non_idempotent = RTEST(flag) ? 1 : 0;

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
    request_options_t *opts;
} execute_args_t;

static VALUE internal_execute_body(VALUE arg) {
    execute_args_t *ea = (execute_args_t *)arg;
    VALUE requests = ea->requests;
    multi_session_t *session = ea->session;
    int *invalid = ea->invalid;
    retry_config_t *retry_cfg = ea->retry_cfg;
    request_options_t *opts = ea->opts;
    int deadline_hit = 0;
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

    if (opts->total_timeout_ms > 0)
        opts->deadline_ms = fast_now_ms() + opts->total_timeout_ms;

    run_multi_loop(session, &cctx, requests, invalid, opts, NULL, count);

    if (!stream && target <= 0 && retry_cfg->max_retries > 0) {
        for (int attempt = 0; attempt < retry_cfg->max_retries; attempt++) {
            if (opts->deadline_ms > 0 && fast_now_ms() >= opts->deadline_ms) {
                deadline_hit = 1;
                break;
            }

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

            long backoff = retry_backoff_ms(retry_cfg, attempt);
            if (opts->deadline_ms > 0) {
                long long left = opts->deadline_ms - fast_now_ms();
                if (left <= 0) {
                    deadline_hit = 1;
                    free(retry_indices);
                    break;
                }
                if (backoff > (long)left)
                    backoff = (long)left;
            }
            retry_delay_sleep(backoff);

            int runnable_count = 0;
            for (int r = 0; r < retry_count; r++) {
                int idx = retry_indices[r];
                request_ctx_t *rc = &session->requests[idx];

                if (!request_ctx_reset_for_retry(rc)) {
                    invalid[idx] = 1;
                    rc->done = 1;
                    rc->setup_error = "Failed to allocate a curl handle for the retry";
                    continue;
                }

                retry_indices[runnable_count++] = idx;
            }

            if (runnable_count > 0) {
                cctx.completed = 0;
                run_multi_loop(session, &cctx, requests, invalid, opts, retry_indices,
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
                response = build_error_response(rc->setup_error ? rc->setup_error
                                                                : "Invalid request configuration",
                                                ERR_INVALID_REQUEST, rc->attempts);
            } else if (!rc->done) {
                response =
                    deadline_hit || (opts->deadline_ms > 0 && fast_now_ms() >= opts->deadline_ms)
                        ? build_error_response("Total timeout exceeded", ERR_DEADLINE, rc->attempts)
                        : build_error_response("Request was not completed", ERR_NOT_COMPLETED,
                                               rc->attempts);
            } else if (rc->curl_result == CURLE_OK) {
                response = build_response(rc);
            } else {
                response = build_error_response_with_code(curl_easy_strerror(rc->curl_result),
                                                          (int)rc->curl_result, rc->attempts);
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

    request_options_t opts;
    int max_conn;
    retry_config_t retry_cfg;
    parse_options(options, &opts, &max_conn, &retry_cfg);

    for (int i = 0; i < count; i++) {
        request_ctx_t probe;
        request_ctx_init(&probe, i);
        if (!request_ctx_prepare_easy(&probe)) {
            request_ctx_free(&probe);
            continue;
        }
        setup_easy_handle(&probe, rb_ary_entry(requests, i), &opts);
        int fatal = probe.setup_error_fatal;
        const char *message = probe.setup_error;
        char buffer[128];
        if (fatal && message) {
            snprintf(buffer, sizeof(buffer), "%s (request %d)", message, i);
        }
        request_ctx_free(&probe);
        if (fatal) {
            VALUE klass = NIL_P(fast_validation_error) ? rb_eArgError : fast_validation_error;
            rb_raise(klass, "%s", buffer);
        }
    }

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
    multi_session_init(&session, curl_multi_init(), count, max_conn, opts.timeout_sec);

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
        .opts = &opts,
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
    fast_share_init();

    for (int i = 0; i < KEY_LAST; i++) {
        fast_ids[i] = rb_intern(KEY_NAMES[i]);
        fast_syms[i] = ID2SYM(fast_ids[i]);
        rb_gc_register_address(&fast_syms[i]);
    }

    for (int i = 0; i < ERR_LAST; i++) {
        error_syms[i] = ID2SYM(rb_intern(ERROR_KIND_NAMES[i]));
        rb_gc_register_address(&error_syms[i]);
    }

    VALUE mFastCurl = rb_define_module("FastCurl");

    if (rb_const_defined_at(mFastCurl, rb_intern("Headers"))) {
        fast_cHeaders = rb_const_get_at(mFastCurl, rb_intern("Headers"));
        rb_gc_register_address(&fast_cHeaders);
    }

    rb_define_module_function(mFastCurl, "execute", rb_fast_curl_execute, -1);
    rb_define_module_function(mFastCurl, "first_execute", rb_fast_curl_first_execute, -1);
    rb_define_module_function(mFastCurl, "stream_execute", rb_fast_curl_stream_execute, -1);
}
