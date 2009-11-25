#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>
#include <assert.h>

#include "config_parser.h"
#include "genhash.h"

#include <memcached/engine.h>

typedef union proxied_engine {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

struct bucket_engine {
    ENGINE_HANDLE_V1 engine;
    bool initialized;
    char *proxied_engine_path;
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
static void bucket_item_release(ENGINE_HANDLE* handle, item* item);
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
static void bucket_reset_stats(ENGINE_HANDLE* handle);
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
    if (cookie) {
        const char *user = e->server->get_auth_data(cookie);
        rv = genhash_find(e->engines, user, strlen(user));
        if (!rv) {
            rv = create_bucket(e, user);
        }
    } else {
        rv = &e->default_engine;
    }
    assert(rv);
    return rv;
}

static inline ENGINE_HANDLE *pe_v0(ENGINE_HANDLE *handle,
                                   const void *cookie) {
    return get_engine(handle, cookie)->v0;
}

static inline ENGINE_HANDLE_V1 *pe_v1(ENGINE_HANDLE *handle,
                                      const void *cookie) {
    return get_engine(handle, cookie)->v1;
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

    bucket_engine.default_engine.v0 = dlopen(se->proxied_engine_path,
                                             RTLD_LAZY | RTLD_LOCAL);
    if (bucket_engine.default_engine.v0 == NULL) {
        const char *msg = dlerror();
        fprintf(stderr, "Failed to open library \"%s\": %s\n",
                se->proxied_engine_path ? se->proxied_engine_path : "self",
                msg ? msg : "unknown error");
        return false;
    }

    void *symbol = dlsym(bucket_engine.default_engine.v0, "create_instance");
    if (symbol == NULL) {
        fprintf(stderr,
                "Could not find symbol \"create_instance\" in %s: %s\n",
                se->proxied_engine_path ? se->proxied_engine_path : "self",
                dlerror());
        return false;
    }
    union tronds_hack {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create = {.create = NULL };

    my_create.voidptr = symbol;
    bucket_engine.new_engine = *my_create.create;

    /* request a instance with protocol version 1 */
    ENGINE_HANDLE *engine = pe_v0(handle, NULL);
    ENGINE_ERROR_CODE error = bucket_engine.new_engine(1,
                                                       bucket_engine.get_server_api,
                                                       &engine);

    if (error != ENGINE_SUCCESS || engine == NULL) {
        fprintf(stderr, "Failed to create instance. Error code: %d\n", error);
        dlclose(handle);
        return false;
    }

    if (engine->interface == 1) {
        bucket_engine.default_engine.v0 = engine;
        bucket_engine.default_engine.v1 = (ENGINE_HANDLE_V1*)engine;
        if (bucket_engine.default_engine.v1->initialize(engine, config_str)
            != ENGINE_SUCCESS) {

            bucket_engine.default_engine.v1->destroy(engine);
            fprintf(stderr, "Failed to initialize instance. Error code: %d\n",
                    error);
            dlclose(handle);
            return ENGINE_FAILED;
        }
    } else {
        fprintf(stderr, "Unsupported interface level\n");
        dlclose(handle);
        return ENGINE_ENOTSUP;
    }

    se->initialized = true;
    return ENGINE_SUCCESS;
}

static void bucket_destroy(ENGINE_HANDLE* handle) {
    struct bucket_engine* se = get_handle(handle);

    if (se->initialized) {
        pe_v1(handle, NULL)->destroy(pe_v0(handle, NULL));
        genhash_free(se->engines);
        se->engines = NULL;
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
    return pe_v1(handle, cookie)->allocate(pe_v0(handle, cookie),
                                           cookie, item,
                                           key, nkey,
                                           nbytes, flags, exptime);
}

static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item* item) {
    return pe_v1(handle, cookie)->remove(pe_v0(handle, cookie),
                                         cookie, item);
}

static void bucket_item_release(ENGINE_HANDLE* handle, item* item) {
    // XXX:  This seems to need a cookie.
    pe_v1(handle, NULL)->release(pe_v0(handle, NULL), item);
}

static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey) {
    return pe_v1(handle, cookie)->get(pe_v0(handle, cookie),
                                      cookie, item, key, nkey);
}

static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const char* stat_key,
                                          int nkey,
                                          ADD_STAT add_stat)
{
    const char *user = bucket_engine.server->get_auth_data(cookie);
    printf("Authenticated as %s\n", user ?: "<nobody>");
    return pe_v1(handle, cookie)->get_stats(pe_v0(handle, cookie),
                                            cookie, stat_key,
                                            nkey, add_stat);
}

static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation) {
    return pe_v1(handle, cookie)->store(pe_v0(handle, cookie), cookie,
                                        item, cas, operation);
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
    return pe_v1(handle, cookie)->arithmetic(pe_v0(handle, cookie),
                                             cookie, key, nkey,
                                             increment, create,
                                             delta, initial, exptime,
                                             cas, result);
}

static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when) {
    return pe_v1(handle, cookie)->flush(pe_v0(handle, cookie),
                                        cookie, when);

}

static void bucket_reset_stats(ENGINE_HANDLE* handle) {
    // XXX:  Needs cookies.
    pe_v1(handle, NULL)->reset_stats(pe_v0(handle, NULL));
}

static ENGINE_ERROR_CODE initalize_configuration(struct bucket_engine *me,
                                                 const char *cfg_str) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "engine",
              .datatype = DT_STRING,
              .value.dt_string = &me->proxied_engine_path },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };

        ret = parse_config(cfg_str, items, stderr);
    }

    return ret;
}

static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response)
{
    return ENGINE_ENOTSUP;
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
