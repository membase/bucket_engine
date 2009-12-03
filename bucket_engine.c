#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>
#include <assert.h>

#include "config_parser.h"
#include "genhash.h"
#include "bucket_engine.h"

#include <memcached/engine.h>

typedef union proxied_engine {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

struct bucket_engine {
    ENGINE_HANDLE_V1 engine;
    bool initialized;
    bool auto_create;
    char *proxied_engine_path;
    char *default_engine_path;
    proxied_engine_t default_engine;
    genhash_t *engines;
    CREATE_INSTANCE new_engine;
    GET_SERVER_API get_server_api;
    SERVER_HANDLE_V1 *server;
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const char* bucket_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str);
static void bucket_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE bucket_item_allocate(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              item **item,
                                              const void* key,
                                              const size_t nkey,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime);
static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item* item);
static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* item);
static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey);
static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          const char *stat_key,
                                          int nkey,
                                          ADD_STAT add_stat);
static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation);
static ENGINE_ERROR_CODE bucket_arithmetic(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const void* key,
                                           const int nkey,
                                           const bool increment,
                                           const bool create,
                                           const uint64_t delta,
                                           const uint64_t initial,
                                           const rel_time_t exptime,
                                           uint64_t *cas,
                                           uint64_t *result);
static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when);
static ENGINE_ERROR_CODE initalize_configuration(struct bucket_engine *me,
                                                 const char *cfg_str);
static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response);
static char* item_get_data(const item* item);
static char* item_get_key(const item* item);
static void item_set_cas(item* item, uint64_t val);
static uint64_t item_get_cas(const item* item);
static uint8_t item_get_clsid(const item* item);

struct bucket_engine bucket_engine = {
    .engine = {
        .interface = {
            .interface = 1
        },
        .get_info = bucket_get_info,
        .initialize = bucket_initialize,
        .destroy = bucket_destroy,
        .allocate = bucket_item_allocate,
        .remove = bucket_item_delete,
        .release = bucket_item_release,
        .get = bucket_get,
        .get_stats = bucket_get_stats,
        .reset_stats = bucket_reset_stats,
        .store = bucket_store,
        .arithmetic = bucket_arithmetic,
        .flush = bucket_flush,
        .unknown_command = bucket_unknown_command,
        .item_get_cas = item_get_cas,
        .item_set_cas = item_set_cas,
        .item_get_key = item_get_key,
        .item_get_data = item_get_data,
        .item_get_clsid = item_get_clsid
    },
    .initialized = false,
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    *handle = (ENGINE_HANDLE*)&bucket_engine;
    bucket_engine.get_server_api = gsapi;
    bucket_engine.server = gsapi(1);
    return ENGINE_SUCCESS;
}

static proxied_engine_t *create_bucket(struct bucket_engine *e,
                                       const char *username) {
    proxied_engine_t *pe = calloc(sizeof(proxied_engine_t), 1);
    assert(pe);

    ENGINE_ERROR_CODE rv = e->new_engine(1, e->get_server_api, &pe->v0);
    // This was already verified, but we'll check it anyway
    assert(pe->v0->interface == 1);
    // XXX:  Need a default config_str
    if (pe->v1->initialize(pe->v0, "") != ENGINE_SUCCESS) {

        pe->v1->destroy(pe->v0);
        fprintf(stderr, "Failed to initialize instance. Error code: %d\n",
                rv);
        return NULL;
    }

    genhash_update(e->engines, username, strlen(username), pe, 0);

    return pe;
}

static inline proxied_engine_t *get_engine(ENGINE_HANDLE *h,
                                           const void *cookie) {
    proxied_engine_t *rv = NULL;
    struct bucket_engine *e = (struct bucket_engine*)h;
    const char *user = e->server->get_auth_data(cookie);
    if (user) {
        rv = genhash_find(e->engines, user, strlen(user));
        if (!rv && e->auto_create) {
            rv = create_bucket(e, user);
        }
    } else {
        rv = e->default_engine.v0 ? &e->default_engine : NULL;
    }
    return rv;
}

static inline struct bucket_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct bucket_engine*)handle;
}

static const char* bucket_get_info(ENGINE_HANDLE* handle) {
    return "Bucket engine v0.1";
}

static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

static void* hash_strdup(const void *k, size_t nkey) {
    void *rv = calloc(nkey, 1);
    assert(rv);
    memcpy(rv, k, nkey);
    return rv;
}

static void* noop_dup(const void* ob, size_t vlen) {
    return (void*)ob;
}

static void engine_free(void* ob) {
    proxied_engine_t *e = (proxied_engine_t *)ob;
    e->v1->destroy(e->v0);
    free(ob);
}

static ENGINE_HANDLE *load_engine(const char *soname, const char *config_str,
                                  CREATE_INSTANCE *create_out) {
    ENGINE_HANDLE *engine = NULL;
    /* Hack to remove the warning from C99 */
    union my_hack {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create = {.create = NULL };

    void *handle = dlopen(soname, RTLD_LAZY | RTLD_LOCAL);
    if (handle == NULL) {
        const char *msg = dlerror();
        fprintf(stderr, "Failed to open library \"%s\": %s\n",
                soname ? soname : "self",
                msg ? msg : "unknown error");
        return NULL;
    }

    void *symbol = dlsym(handle, "create_instance");
    if (symbol == NULL) {
        fprintf(stderr,
                "Could not find symbol \"create_instance\" in %s: %s\n",
                soname ? soname : "self",
                dlerror());
        return NULL;
    }
    my_create.voidptr = symbol;
    if (create_out) {
        *create_out = my_create.create;
    }

    /* request a instance with protocol version 1 */
    ENGINE_ERROR_CODE error = (*my_create.create)(1,
                                                  bucket_engine.get_server_api,
                                                  &engine);

    if (error != ENGINE_SUCCESS || engine == NULL) {
        fprintf(stderr, "Failed to create instance. Error code: %d\n", error);
        dlclose(handle);
        return NULL;
    }

    if (engine->interface == 1) {
        ENGINE_HANDLE_V1 *v1 = (ENGINE_HANDLE_V1*)engine;
        if (v1->initialize(engine, config_str) != ENGINE_SUCCESS) {
            v1->destroy(engine);
            fprintf(stderr, "Failed to initialize instance. Error code: %d\n",
                    error);
            dlclose(handle);
            return NULL;
        }
    } else {
        fprintf(stderr, "Unsupported interface level\n");
        dlclose(handle);
        return NULL;
    }

    return engine;
}

static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str) {
    struct bucket_engine* se = get_handle(handle);

    assert(!se->initialized);

    ENGINE_ERROR_CODE ret = initalize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    static struct hash_ops my_hash_ops = {
        .hashfunc = genhash_string_hash,
        .hasheq = my_hash_eq,
        .dupKey = hash_strdup,
        .dupValue = noop_dup,
        .freeKey = free,
        .freeValue = engine_free
    };

    se->engines = genhash_init(1, my_hash_ops);
    if (se->engines == NULL) {
        return ENGINE_ENOMEM;
    }

    // Try loading an engine just to see if we can.
    ENGINE_HANDLE *eh = load_engine(se->proxied_engine_path, "",
                                    &se->new_engine);
    if (!eh) {
        return ENGINE_FAILED;
    }
    // Immediately shut it down.
    ENGINE_HANDLE_V1 *ehv1 = (ENGINE_HANDLE_V1*)eh;
    ehv1->destroy(eh);

    // Initialization is useful to know if we *can* start up an
    // engine, but we check flags here to see if we should have and
    // shut it down if not.
    if (se->default_engine_path) {
        se->default_engine.v0 = load_engine(se->default_engine_path, "", NULL);
    }

    se->initialized = true;
    return ENGINE_SUCCESS;
}

static void bucket_destroy(ENGINE_HANDLE* handle) {
    struct bucket_engine* se = get_handle(handle);

    if (se->initialized) {
        proxied_engine_t *e = get_engine(handle, NULL);
        if (e) {
            e->v1->destroy(e->v0);
            e->v0 = NULL;
        }
        genhash_free(se->engines);
        se->engines = NULL;
        se->default_engine_path = NULL;
        se->initialized = false;
    }
}

static ENGINE_ERROR_CODE bucket_item_allocate(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              item **item,
                                              const void* key,
                                              const size_t nkey,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->allocate(e->v0, cookie, item, key,
                               nkey, nbytes, flags, exptime);
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item* item) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->remove(e->v0, cookie, item);
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* item) {
    proxied_engine_t *e = get_engine(handle, cookie);
    assert(e);
    if (e) {
        e->v1->release(e->v0, cookie, item);
    }
}

static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get(e->v0, cookie, item, key, nkey);
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const char* stat_key,
                                          int nkey,
                                          ADD_STAT add_stat)
{
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get_stats(e->v0, cookie, stat_key, nkey, add_stat);
    } else {
        return ENGINE_FAILED;
    }
}

static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->store(e->v0, cookie, item, cas, operation);
    } else {
        return ENGINE_NOT_STORED;
    }
}

static ENGINE_ERROR_CODE bucket_arithmetic(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const void* key,
                                           const int nkey,
                                           const bool increment,
                                           const bool create,
                                           const uint64_t delta,
                                           const uint64_t initial,
                                           const rel_time_t exptime,
                                           uint64_t *cas,
                                           uint64_t *result) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->arithmetic(e->v0, cookie, key, nkey,
                                 increment, create, delta, initial,
                                 exptime, cas, result);
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when) {
    proxied_engine_t *e = get_engine(handle, cookie);
    return e->v1->flush(e->v0, cookie, when);
}

static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->reset_stats(e->v0, cookie);
    }
}

static ENGINE_ERROR_CODE initalize_configuration(struct bucket_engine *me,
                                                 const char *cfg_str) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    me->auto_create = true;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "engine",
              .datatype = DT_STRING,
              .value.dt_string = &me->proxied_engine_path },
            { .key = "default",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_engine_path },
            { .key = "auto_create",
              .datatype = DT_BOOL,
              .value.dt_bool = &me->auto_create },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };

        ret = parse_config(cfg_str, items, stderr);
    }

    if (me->default_engine_path == NULL) {
        me->default_engine_path = me->proxied_engine_path;
    }

    if (strcasecmp(me->default_engine_path, "null") == 0) {
        me->default_engine_path = NULL;
    }

    return ret;
}

#define EXTRACT_KEY(req, out)                                       \
    char keyz[req->message.header.request.keylen + 1];              \
    memcpy(keyz, ((void*)request) + sizeof(req->message.header),    \
           req->message.header.request.keylen);                     \
    keyz[req->message.header.request.keylen] = 0x00;

static ENGINE_ERROR_CODE handle_create_bucket(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    protocol_binary_request_create_bucket *breq =
        (protocol_binary_request_create_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    return create_bucket(e, keyz) ? ENGINE_SUCCESS : ENGINE_FAILED;
}

static bool authorized(ENGINE_HANDLE* handle,
                       const void* cookie) {
    // XXX:  May not be true.
    return true;
}

static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response)
{
    if (!authorized(handle, cookie)) {
        return ENGINE_ENOTSUP;
    }

    ENGINE_ERROR_CODE rv = ENGINE_ENOTSUP;
    switch(request->request.opcode) {
    case CREATE_BUCKET:
        rv = handle_create_bucket(handle, cookie, request, response);
        break;
    }
    return rv;
}

static char* item_get_data(const item* item) {
    return bucket_engine.default_engine.v1->item_get_data(item);
}

static char* item_get_key(const item* item) {
    return bucket_engine.default_engine.v1->item_get_key(item);
}

static void item_set_cas(item* item, uint64_t val) {
    bucket_engine.default_engine.v1->item_set_cas(item, val);
}

static uint64_t item_get_cas(const item* item) {
    return bucket_engine.default_engine.v1->item_get_cas(item);
}

static uint8_t item_get_clsid(const item* item) {
    return bucket_engine.default_engine.v1->item_get_clsid(item);
}
