#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <dlfcn.h>
#include <string.h>
#include <pthread.h>
#ifndef WIN32
#include <arpa/inet.h>
#else
#include <winsock.h>
#endif

#include <assert.h>

#include <memcached/engine.h>
#include <memcached/genhash.h>

#include "bucket_engine.h"

typedef union proxied_engine {
    ENGINE_HANDLE    *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

typedef struct proxied_engine_handle {
    proxied_engine_t     pe;
    struct thread_stats *stats;
    int                  refcount;
    bool                 valid;
    TAP_ITERATOR         tap_iterator;
    /* ON_DISCONNECT handling */
    bool                 wants_disconnects;
    EVENT_CALLBACK       cb;
    const void          *cb_data;
} proxied_engine_handle_t;

typedef struct engine_specific {
    proxied_engine_handle_t *peh;
    void                    *engine_specific;
} engine_specific_t;

struct bucket_engine {
    ENGINE_HANDLE_V1 engine;
    SERVER_HANDLE_V1 *upstream_server;
    bool initialized;
    bool has_default;
    bool auto_create;
    char *default_engine_path;
    char *admin_user;
    char *default_bucket_name;
    proxied_engine_handle_t default_engine;
    pthread_mutex_t engines_mutex;
    pthread_mutex_t retention_mutex;
    genhash_t *engines;
    GET_SERVER_API get_server_api;
    SERVER_HANDLE_V1 server;
    SERVER_CALLBACK_API callback_api;
    SERVER_EXTENSION_API extension_api;
    SERVER_COOKIE_API cookie_api;

    union {
      engine_info engine_info;
      char buffer[sizeof(engine_info) +
                  (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const engine_info* bucket_get_info(ENGINE_HANDLE* handle);

static const char *get_default_bucket_config(void);

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
                                            const void* key,
                                            const size_t nkey,
                                            uint64_t cas,
                                            uint16_t vbucket);
static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* item);
static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket);
static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          const char *stat_key,
                                          int nkey,
                                          ADD_STAT add_stat);
static void *bucket_get_stats_struct(ENGINE_HANDLE* handle,
                                                    const void *cookie);
static ENGINE_ERROR_CODE bucket_aggregate_stats(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                void (*callback)(void*, void*),
                                                void *stats);
static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket);
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
                                           uint64_t *result,
                                           uint16_t vbucket);
static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when);
static ENGINE_ERROR_CODE initialize_configuration(struct bucket_engine *me,
                                                  const char *cfg_str);
static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response);

static bool bucket_get_item_info(ENGINE_HANDLE *handle,
                                 const void *cookie,
                                 const item* item,
                                 item_info *item_info);

static void bucket_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item *item, uint64_t cas);

static ENGINE_ERROR_CODE bucket_tap_notify(ENGINE_HANDLE* handle,
                                           const void *cookie,
                                           void *engine_specific,
                                           uint16_t nengine,
                                           uint8_t ttl,
                                           uint16_t tap_flags,
                                           tap_event_t tap_event,
                                           uint32_t tap_seqno,
                                           const void *key,
                                           size_t nkey,
                                           uint32_t flags,
                                           uint32_t exptime,
                                           uint64_t cas,
                                           const void *data,
                                           size_t ndata,
                                           uint16_t vbucket);

static TAP_ITERATOR bucket_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* client, size_t nclient,
                                            uint32_t flags,
                                            const void* userdata, size_t nuserdata);

static size_t bucket_errinfo(ENGINE_HANDLE *handle, const void* cookie,
                             char *buffer, size_t buffsz);

static ENGINE_HANDLE *load_engine(const char *soname, const char *config_str,
                                  CREATE_INSTANCE *create_out);

struct bucket_engine bucket_engine = {
    .engine = {
        .interface = {
            .interface = 1
        },
        .get_info         = bucket_get_info,
        .initialize       = bucket_initialize,
        .destroy          = bucket_destroy,
        .allocate         = bucket_item_allocate,
        .remove           = bucket_item_delete,
        .release          = bucket_item_release,
        .get              = bucket_get,
        .store            = bucket_store,
        .arithmetic       = bucket_arithmetic,
        .flush            = bucket_flush,
        .get_stats        = bucket_get_stats,
        .reset_stats      = bucket_reset_stats,
        .get_stats_struct = bucket_get_stats_struct,
        .aggregate_stats  = bucket_aggregate_stats,
        .unknown_command  = bucket_unknown_command,
        .tap_notify       = bucket_tap_notify,
        .get_tap_iterator = bucket_get_tap_iterator,
        .item_set_cas     = bucket_item_set_cas,
        .get_item_info    = bucket_get_item_info,
        .errinfo          = bucket_errinfo
    },
    .initialized = false,
    .info.engine_info = {
        .description = "Bucket engine v0.2",
        .num_features = 1,
        .features = {
            [0].feature = ENGINE_FEATURE_MULTI_TENANCY
        }
    },
};

/* Internal utility functions */

static const char *get_default_bucket_config() {
    const char *config = getenv("MEMCACHED_DEFAULT_BUCKET_CONFIG");
    return config != NULL ? config : "";
}

static SERVER_HANDLE_V1 *bucket_get_server_api(void) {
    return &bucket_engine.server;
}

struct bucket_find_by_handle_data {
    ENGINE_HANDLE *needle;
    proxied_engine_handle_t *peh;
};

static void find_bucket_by_engine(const void* key, size_t nkey,
                                  const void *val, size_t nval,
                                  void *args) {
    struct bucket_find_by_handle_data *find_data = args;
    assert(find_data);
    assert(find_data->needle);

    const proxied_engine_handle_t *peh = val;
    if (find_data->needle == peh->pe.v0) {
        find_data->peh = (proxied_engine_handle_t *)peh;
    }
}

static void bucket_register_callback(ENGINE_HANDLE *eh,
                                     ENGINE_EVENT_TYPE type,
                                     EVENT_CALLBACK cb, const void *cb_data) {

    /* For simplicity, we're not going to test every combination until
       we need them. */
    assert(type == ON_DISCONNECT);

    /* Assume this always happens while holding the hash table lock. */

    struct {
        ENGINE_HANDLE *needle;
        proxied_engine_handle_t *peh;
    } find_data = { eh, NULL };

    genhash_iter(bucket_engine.engines, find_bucket_by_engine, &find_data);

    if (find_data.peh) {
        find_data.peh->wants_disconnects = true;
        find_data.peh->cb = cb;
        find_data.peh->cb_data = cb_data;
    }
}

static void bucket_perform_callbacks(ENGINE_EVENT_TYPE type,
                                     const void *data, const void *cookie) {
    abort(); /* Not implemented */
}

static void bucket_store_engine_specific(const void *cookie, void *engine_data) {
    engine_specific_t *es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    // There should *always* be an es here, because a bucket is trying
    // to store data.  A bucket won't be there without an es.
    assert(es);
    es->engine_specific = engine_data;
}

static void* bucket_get_engine_specific(const void *cookie) {
    engine_specific_t *es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    return es ? es->engine_specific : NULL;
}

static bool bucket_register_extension(extension_type_t type,
                                      void *extension) {
    return false;
}

static void bucket_unregister_extension(extension_type_t type, void *extension) {
    abort(); /* No extensions registered, none can unregister */
}

static void* bucket_get_extension(extension_type_t type) {
    return bucket_engine.upstream_server->extension->get_extension(type);
}

/* Engine API functions */

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    *handle = (ENGINE_HANDLE*)&bucket_engine;
    bucket_engine.upstream_server = gsapi();
    bucket_engine.server = *bucket_engine.upstream_server;
    bucket_engine.get_server_api = bucket_get_server_api;

    /* Use our own callback API for inferior engines */
    bucket_engine.callback_api.register_callback = bucket_register_callback;
    bucket_engine.callback_api.perform_callbacks = bucket_perform_callbacks;
    bucket_engine.server.callback = &bucket_engine.callback_api;

    /* Same for extensions */
    bucket_engine.extension_api.register_extension = bucket_register_extension;
    bucket_engine.extension_api.unregister_extension = bucket_unregister_extension;
    bucket_engine.extension_api.get_extension = bucket_get_extension;
    bucket_engine.server.extension = &bucket_engine.extension_api;

    /* Override engine specific */
    bucket_engine.cookie_api = *bucket_engine.upstream_server->cookie;
    bucket_engine.server.cookie = &bucket_engine.cookie_api;
    bucket_engine.server.cookie->store_engine_specific = bucket_store_engine_specific;
    bucket_engine.server.cookie->get_engine_specific = bucket_get_engine_specific;

    return ENGINE_SUCCESS;
}

static void release_handle(proxied_engine_handle_t *peh) {
    if (peh && pthread_mutex_lock(&bucket_engine.retention_mutex) == 0) {
        assert(peh->refcount > 0);
        if (--peh->refcount == 0) {
            // We should never free the default engine.
            assert(peh != &bucket_engine.default_engine);
            peh->pe.v1->destroy(peh->pe.v0);
            bucket_engine.upstream_server->stat->release_stats(peh->stats);
            free(peh);
        }
        pthread_mutex_unlock(&bucket_engine.retention_mutex);
    }
}

static proxied_engine_handle_t* retain_handle(proxied_engine_handle_t *peh) {
    proxied_engine_handle_t *rv = NULL;
    if (peh && pthread_mutex_lock(&bucket_engine.retention_mutex) == 0) {
        if (peh->valid) {
            ++peh->refcount;
            assert(peh->refcount > 0);
            rv = peh;
        }
        pthread_mutex_unlock(&bucket_engine.retention_mutex);
    }
    return rv;
}

static bool has_valid_bucket_name(const char *n) {
    bool rv = n[0] != 0;
    for (; *n; n++) {
        rv &= isalpha(*n) || isdigit(*n) || *n == '.' || *n == '%' || *n == '_' || *n == '-';
    }
    return rv;
}

static ENGINE_ERROR_CODE create_bucket(struct bucket_engine *e,
                                       const char *bucket_name,
                                       const char *path,
                                       const char *config,
                                       proxied_engine_handle_t **e_out) {

    if (!has_valid_bucket_name(bucket_name)) {
        return ENGINE_EINVAL;
    }

    *e_out = calloc(sizeof(proxied_engine_handle_t), 1);
    proxied_engine_handle_t *peh = *e_out;
    assert(peh);
    peh->stats = e->upstream_server->stat->new_stats();
    assert(peh->stats);
    peh->refcount = 1;
    peh->valid = true;

    ENGINE_ERROR_CODE rv = ENGINE_FAILED;

    if (pthread_mutex_lock(&e->engines_mutex) != 0) {
        release_handle(peh);
        return rv;
    }

    peh->pe.v0 = load_engine(path, NULL, NULL);

    if (!peh->pe.v0) {
        release_handle(peh);
        pthread_mutex_unlock(&e->engines_mutex);
        return rv;
    }

    if (genhash_find(e->engines, bucket_name, strlen(bucket_name)) == NULL) {
        genhash_update(e->engines, bucket_name, strlen(bucket_name), peh, 0);

        // This was already verified, but we'll check it anyway
        assert(peh->pe.v0->interface == 1);
        if (peh->pe.v1->initialize(peh->pe.v0, config) != ENGINE_SUCCESS) {
            peh->pe.v1->destroy(peh->pe.v0);
            genhash_delete_all(e->engines, bucket_name, strlen(bucket_name));
            fprintf(stderr, "Failed to initialize instance. Error code: %d\n",
                    rv);
            pthread_mutex_unlock(&e->engines_mutex);
            return ENGINE_FAILED;
        }

        rv = ENGINE_SUCCESS;
    } else {
        rv = ENGINE_KEY_EEXISTS;
    }

    release_handle(peh);

    pthread_mutex_unlock(&e->engines_mutex);

    return rv;
}

static inline proxied_engine_handle_t *get_engine_handle(ENGINE_HANDLE *h,
                                                         const void *cookie) {
    struct bucket_engine *e = (struct bucket_engine*)h;
    engine_specific_t *es = e->upstream_server->cookie->get_engine_specific(cookie);
    if (!es) {
        return NULL;
    }
    proxied_engine_handle_t *peh = es->peh;
    if (peh && !peh->valid) {
        release_handle(es->peh);
        e->upstream_server->cookie->store_engine_specific(cookie, NULL);
        free(es);
        return NULL;
    }

    return peh ? peh : (e->default_engine.pe.v0 ? &e->default_engine : NULL);
}

static inline void set_engine_handle(ENGINE_HANDLE *h, const void *cookie,
                                     proxied_engine_handle_t *peh) {
    engine_specific_t *es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    if (!es) {
        es = calloc(1, sizeof(engine_specific_t));
        assert(es);
        struct bucket_engine *e = (struct bucket_engine*)h;
        e->upstream_server->cookie->store_engine_specific(cookie, es);
    }
    // out with the old
    release_handle(es->peh);
    // In with the new
    es->peh = retain_handle(peh);
}

static inline proxied_engine_t *get_engine(ENGINE_HANDLE *h,
                                           const void *cookie) {
    proxied_engine_handle_t *peh = get_engine_handle(h, cookie);
    return peh ? &peh->pe : NULL;
}

static inline struct bucket_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct bucket_engine*)handle;
}

static const engine_info* bucket_get_info(ENGINE_HANDLE* handle) {
    return &(get_handle(handle)->info.engine_info);
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

static void* refcount_dup(const void* ob, size_t vlen) {
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    assert(peh);
    if (pthread_mutex_lock(&bucket_engine.retention_mutex) == 0) {
        peh->refcount++;
        pthread_mutex_unlock(&bucket_engine.retention_mutex);
    }
    return (void*)ob;
}

static void engine_hash_free(void* ob) {
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    peh->valid = false;
    release_handle(peh);
}

static ENGINE_HANDLE *load_engine(const char *soname, const char *config_str,
                                  CREATE_INSTANCE *create_out) {
    ENGINE_HANDLE *engine = NULL;
    /* Hack to remove the warning from C99 */
    union my_hack {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create = {.create = NULL };

    void *handle = dlopen(soname, RTLD_NOW | RTLD_LOCAL);
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

    if (config_str) {
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
    }

    return engine;
}

static void handle_disconnect(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data) {
    struct bucket_engine *e = (struct bucket_engine*)cb_data;


    engine_specific_t *es =
        e->upstream_server->cookie->get_engine_specific(cookie);
    proxied_engine_handle_t *peh = es ? es->peh : NULL;

    if (peh && peh->wants_disconnects) {
        peh->cb(cookie, type, event_data, peh->cb_data);
    }

    // Free up the engine we were using.
    release_handle(peh);
    free(es);
    e->upstream_server->cookie->store_engine_specific(cookie, NULL);
}

static void handle_connect(const void *cookie,
                           ENGINE_EVENT_TYPE type,
                           const void *event_data,
                           const void *cb_data) {
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    proxied_engine_handle_t *peh = NULL;
    if (e->default_bucket_name != NULL) {
        // Assign a default named bucket (if there is one).
        if (pthread_mutex_lock(&e->engines_mutex) == 0) {
            peh = genhash_find(e->engines, e->default_bucket_name,
                               strlen(e->default_bucket_name));
            pthread_mutex_unlock(&e->engines_mutex);
        }
        if (!peh && e->auto_create) {
            // XXX:  Need default config.
            create_bucket(e, e->default_bucket_name,
                          e->default_engine_path,
                          get_default_bucket_config(), &peh);
        }
    } else {
        // Assign the default bucket (if there is one).
        peh = e->default_engine.pe.v0 ? &e->default_engine : NULL;
    }

    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
}

static void handle_auth(const void *cookie,
                        ENGINE_EVENT_TYPE type,
                        const void *event_data,
                        const void *cb_data) {
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    const auth_data_t *auth_data = (const auth_data_t*)event_data;
    proxied_engine_handle_t *peh = NULL;
    if (pthread_mutex_lock(&e->engines_mutex) == 0) {
        peh = genhash_find(e->engines, auth_data->username, strlen(auth_data->username));
        pthread_mutex_unlock(&e->engines_mutex);
    } else {
        return;
    }
    if (!peh && e->auto_create) {
        create_bucket(e, auth_data->username, e->default_engine_path,
                      auth_data->config ? auth_data->config : "", &peh);
    }
    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
}

static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str) {
    struct bucket_engine* se = get_handle(handle);

    assert(!se->initialized);

    if (pthread_mutex_init(&se->engines_mutex, NULL) != 0) {
        fprintf(stderr, "Error initializing mutex for bucket engine.\n");
        return ENGINE_FAILED;
    }

    if (pthread_mutex_init(&se->retention_mutex, NULL) != 0) {
        fprintf(stderr, "Error initializing retention mutex for bucket engine.\n");
        return ENGINE_FAILED;
    }

    ENGINE_ERROR_CODE ret = initialize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    static struct hash_ops my_hash_ops = {
        .hashfunc = genhash_string_hash,
        .hasheq = my_hash_eq,
        .dupKey = hash_strdup,
        .dupValue = refcount_dup,
        .freeKey = free,
        .freeValue = engine_hash_free
    };

    se->engines = genhash_init(1, my_hash_ops);
    if (se->engines == NULL) {
        return ENGINE_ENOMEM;
    }

    // Initialization is useful to know if we *can* start up an
    // engine, but we check flags here to see if we should have and
    // shut it down if not.
    if (se->has_default) {
        memset(&se->default_engine, 0, sizeof(se->default_engine));
        se->default_engine.refcount = 1;
        se->default_engine.valid = true;
        se->default_engine.pe.v0 = load_engine(se->default_engine_path, NULL, NULL);

        ENGINE_HANDLE_V1 *dv1 = (ENGINE_HANDLE_V1*)se->default_engine.pe.v0;
        if (!dv1) {
            return ENGINE_FAILED;
        }

        if (dv1->initialize(se->default_engine.pe.v0, config_str) != ENGINE_SUCCESS) {
            dv1->destroy(se->default_engine.pe.v0);
            return ENGINE_FAILED;
        }
    }

    se->upstream_server->callback->register_callback(handle, ON_CONNECT,
                                                     handle_connect, se);
    se->upstream_server->callback->register_callback(handle, ON_AUTH,
                                                     handle_auth, se);
    se->upstream_server->callback->register_callback(handle, ON_DISCONNECT,
                                                     handle_disconnect, se);

    se->initialized = true;
    return ENGINE_SUCCESS;
}

static void bucket_destroy(ENGINE_HANDLE* handle) {
    struct bucket_engine* se = get_handle(handle);

    if (se->initialized) {
        genhash_free(se->engines);
        se->engines = NULL;
        free(se->default_engine_path);
        se->default_engine_path = NULL;
        free(se->admin_user);
        se->admin_user = NULL;
        free(se->default_bucket_name);
        se->default_bucket_name = NULL;
        pthread_mutex_destroy(&se->engines_mutex);
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
        return ENGINE_DISCONNECT;
    }
}

static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const size_t nkey,
                                            uint64_t cas,
                                            uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->remove(e->v0, cookie, key, nkey, cas, vbucket);
    } else {
        return ENGINE_DISCONNECT;
    }
}

static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* item) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->release(e->v0, cookie, item);
    }
}

static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get(e->v0, cookie, item, key, nkey, vbucket);
    } else {
        return ENGINE_DISCONNECT;
    }
}

struct bucket_list {
    char *name;
    int namelen;
    proxied_engine_handle_t *peh;
    struct bucket_list *next;
};

static void add_engine(const void *key, size_t nkey,
                  const void *val, size_t nval,
                  void *arg) {
    struct bucket_list **blist_ptr = (struct bucket_list **)arg;
    struct bucket_list *n = calloc(sizeof(struct bucket_list), 1);
    n->name = (char*)key;
    n->namelen = nkey;
    n->peh = (proxied_engine_handle_t*) val;
    assert(n->peh);
    retain_handle(n->peh);
    n->next = *blist_ptr;
    *blist_ptr = n;
}

static bool list_buckets(struct bucket_engine *e, struct bucket_list **blist) {
    if (pthread_mutex_lock(&e->engines_mutex) == 0) {
        genhash_iter(e->engines, add_engine, blist);
        pthread_mutex_unlock(&e->engines_mutex);
        return true;
    } else {
        return false;
    }
}

static void bucket_list_free(struct bucket_list *blist) {
    struct bucket_list *p = blist;
    while (p) {
        release_handle(p->peh);
        struct bucket_list *tmp = p->next;
        free(p);
        p = tmp;
    }
}

static ENGINE_ERROR_CODE bucket_aggregate_stats(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                void (*callback)(void*, void*),
                                                void *stats) {

    struct bucket_engine *e = (struct bucket_engine*)handle;
    struct bucket_list *blist = NULL;
    if (! list_buckets(e, &blist)) {
        return ENGINE_FAILED;
    }

    struct bucket_list *p = blist;
    while (p) {
        callback(p->peh->stats, stats);
        p = p->next;
    }

    bucket_list_free(blist);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const char* stat_key,
                                          int nkey,
                                          ADD_STAT add_stat) {
    ENGINE_ERROR_CODE rc = ENGINE_DISCONNECT;
    proxied_engine_t *e = get_engine(handle, cookie);

    if (e) {
        rc = e->v1->get_stats(e->v0, cookie, stat_key, nkey, add_stat);
        proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
        if (nkey == 0) {
            char statval[20];
            snprintf(statval, 20, "%d", peh->refcount - 1);
            add_stat("bucket_conns", strlen("bucket_conns"), statval,
                     strlen(statval), cookie);
        }
    }
    return rc;
}

static void *bucket_get_stats_struct(ENGINE_HANDLE* handle,
                                     const void* cookie) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh != NULL && peh->valid) {
        return peh->stats;
    } else {
        return NULL;
    }
}

static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->store(e->v0, cookie, item, cas, operation, vbucket);
    } else {
        return ENGINE_DISCONNECT;
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
                                           uint64_t *result,
                                           uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->arithmetic(e->v0, cookie, key, nkey,
                                 increment, create, delta, initial,
                                 exptime, cas, result, vbucket);
    } else {
        return ENGINE_DISCONNECT;
    }
}

static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
      return e->v1->flush(e->v0, cookie, when);
    } else {
      return ENGINE_DISCONNECT;
    }
}

static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->reset_stats(e->v0, cookie);
    }
}

static bool bucket_get_item_info(ENGINE_HANDLE *handle,
                                 const void *cookie,
                                 const item* item,
                                 item_info *item_info) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get_item_info(e->v0, cookie, item, item_info);
    } else {
        return false;
    }
}

static void bucket_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item *item, uint64_t cas) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->item_set_cas(e->v0, cookie, item, cas);
    }
}

static ENGINE_ERROR_CODE bucket_tap_notify(ENGINE_HANDLE* handle,
                                           const void *cookie,
                                           void *engine_specific,
                                           uint16_t nengine,
                                           uint8_t ttl,
                                           uint16_t tap_flags,
                                           tap_event_t tap_event,
                                           uint32_t tap_seqno,
                                           const void *key,
                                           size_t nkey,
                                           uint32_t flags,
                                           uint32_t exptime,
                                           uint64_t cas,
                                           const void *data,
                                           size_t ndata,
                                           uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->tap_notify(e->v0, cookie, engine_specific,
                                 nengine, ttl, tap_flags, tap_event, tap_seqno,
                                 key, nkey, flags, exptime, cas, data, ndata, vbucket);
    } else {
        return ENGINE_DISCONNECT;
    }
}

static tap_event_t bucket_tap_iterator_shim(ENGINE_HANDLE* handle,
                                            const void *cookie,
                                            item **item,
                                            void **engine_specific,
                                            uint16_t *nengine_specific,
                                            uint8_t *ttl,
                                            uint16_t *flags,
                                            uint32_t *seqno,
                                            uint16_t *vbucket) {
    proxied_engine_handle_t *e = get_engine_handle(handle, cookie);
    if (e && e->tap_iterator) {
        assert(e->pe.v0 != handle);
        return e->tap_iterator(e->pe.v0, cookie, item,
                               engine_specific, nengine_specific,
                               ttl, flags, seqno, vbucket);
    } else {
        return TAP_DISCONNECT;
    }
}

static TAP_ITERATOR bucket_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* client, size_t nclient,
                                            uint32_t flags,
                                            const void* userdata, size_t nuserdata) {
    proxied_engine_handle_t *e = get_engine_handle(handle, cookie);
    if (e) {
        e->tap_iterator = e->pe.v1->get_tap_iterator(e->pe.v0, cookie,
                                                     client, nclient,
                                                     flags, userdata, nuserdata);
        return e->tap_iterator ? bucket_tap_iterator_shim : NULL;
    } else {
        return NULL;
    }
}

static size_t bucket_errinfo(ENGINE_HANDLE *handle, const void* cookie,
                             char *buffer, size_t buffsz) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->errinfo
            ? e->v1->errinfo(e->v0, cookie, buffer, buffsz)
            : 0;
    } else {
        return 0;
    }
}

static ENGINE_ERROR_CODE initialize_configuration(struct bucket_engine *me,
                                                  const char *cfg_str) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    me->auto_create = true;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "engine",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_engine_path },
            { .key = "admin",
              .datatype = DT_STRING,
              .value.dt_string = &me->admin_user },
            { .key = "default",
              .datatype = DT_BOOL,
              .value.dt_bool = &me->has_default },
            { .key = "default_bucket_name",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_bucket_name },
            { .key = "auto_create",
              .datatype = DT_BOOL,
              .value.dt_bool = &me->auto_create },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };

        ret = me->upstream_server->core->parse_config(cfg_str, items, stderr);
    }

    return ret;
}

#define EXTRACT_KEY(req, out)                                       \
    char keyz[ntohs(req->message.header.request.keylen) + 1];       \
    memcpy(keyz, ((char*)request) + sizeof(req->message.header),    \
           ntohs(req->message.header.request.keylen));              \
    keyz[ntohs(req->message.header.request.keylen)] = 0x00;

static ENGINE_ERROR_CODE handle_create_bucket(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    protocol_binary_request_create_bucket *breq =
        (protocol_binary_request_create_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    size_t bodylen = ntohl(breq->message.header.request.bodylen)
        - ntohs(breq->message.header.request.keylen);
    assert(bodylen < (1 << 16)); // 64k ought to be enough for anybody
    char spec[bodylen + 1];
    memcpy(spec, ((char*)request) + sizeof(breq->message.header)
           + ntohs(breq->message.header.request.keylen), bodylen);
    spec[bodylen] = 0x00;

    if (spec[0] == 0) {
        const char *msg = "Invalid request.";
        response(msg, strlen(msg), "", 0, "", 0, 0,
                 PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        return ENGINE_SUCCESS;
    }
    char *config = "";
    if (strlen(spec) < bodylen) {
        config = spec + strlen(spec)+1;
    }

    proxied_engine_handle_t *peh = NULL;
    ENGINE_ERROR_CODE ret = create_bucket(e, keyz, spec, config ? config : "", &peh);

    const char *msg = "";
    protocol_binary_response_status rc = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch(ret) {
    case ENGINE_SUCCESS:
        // Defaults as above.
        break;
    case ENGINE_KEY_EEXISTS:
        msg = "Bucket exists";
        rc = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        break;
    default:
        msg = "Error creating bucket";
        rc = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    }

    response(msg, strlen(msg), "", 0, "", 0, 0, rc, 0, cookie);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE handle_delete_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    int upd = 0;
    if (pthread_mutex_lock(&e->engines_mutex) == 0) {
        upd = genhash_delete_all(e->engines, keyz, strlen(keyz));
        assert(genhash_find(e->engines, keyz, strlen(keyz)) == NULL);
        pthread_mutex_unlock(&e->engines_mutex);
    } else {
        return ENGINE_FAILED;
    }

    if (upd > 0) {
        response("", 0, "", 0, "", 0, 0, 0, 0, cookie);
    } else {
        const char *msg = "Not found.";
        response(msg, strlen(msg),
                 "", 0, "", 0,
                 0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                 0, cookie);
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE handle_list_buckets(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;

    // Accumulate the current bucket list.
    struct bucket_list *blist = NULL;
    if (! list_buckets(e, &blist)) {
        return ENGINE_FAILED;
    }

    int len = 0, n = 0;
    struct bucket_list *p = blist;
    while (p) {
        len += p->namelen;
        n++;
        p = p->next;
    }

    // Now turn it into a space-separated list.
    char *blist_txt = calloc(sizeof(char), n + len);
    assert(blist_txt);
    p = blist;
    while (p) {
        strncat(blist_txt, p->name, p->namelen);
        if (p->next) {
            strcat(blist_txt, " ");
        }
        p = p->next;
    }

    bucket_list_free(blist);

    // Response body will be "" in the case of an empty response.
    // Otherwise, it needs to account for the trailing space of the
    // above append code.
    response("", 0, "", 0, blist_txt,
             n == 0 ? 0 : (sizeof(char) * n + len) - 1,
             0, 0, 0, cookie);
    free(blist_txt);

    return ENGINE_SUCCESS;
}

static bool authorized(ENGINE_HANDLE* handle,
                       const void* cookie) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    bool rv = false;
    if (e->admin_user) {
        auth_data_t data = {.username = 0, .config = 0};
        e->upstream_server->cookie->get_auth_data(cookie, &data);
        if (data.username) {
            rv = strcmp(data.username, e->admin_user) == 0;
        }
    }
    return rv;
}

static ENGINE_ERROR_CODE handle_expand_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    proxied_engine_t *proxied = NULL;
    if (pthread_mutex_lock(&e->engines_mutex) == 0) {
        proxied = genhash_find(e->engines, keyz, strlen(keyz));
        pthread_mutex_unlock(&e->engines_mutex);
    } else {
        return ENGINE_FAILED;
    }

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    if (proxied) {
        rv = proxied->v1->unknown_command(handle, cookie, request, response);
    } else {
        const char *msg = "Engine not found";
        response(msg, strlen(msg),
                 "", 0, "", 0,
                 0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                 0, cookie);
    }

    return rv;
}

static ENGINE_ERROR_CODE handle_select_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    struct bucket_engine *e = (struct bucket_engine*)handle;
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    proxied_engine_handle_t *proxied = NULL;
    if (pthread_mutex_lock(&e->engines_mutex) == 0) {
        proxied = genhash_find(e->engines, keyz, strlen(keyz));
        pthread_mutex_unlock(&e->engines_mutex);
    } else {
        return ENGINE_FAILED;
    }

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    if (proxied) {
        // Free up the currently held engine.
        set_engine_handle(handle, cookie, proxied);
        response("", 0, "", 0, "", 0, 0, 0, 0, cookie);
    } else {
        const char *msg = "Engine not found";
        response(msg, strlen(msg),
                 "", 0, "", 0,
                 0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                 0, cookie);
    }

    return rv;
}

static inline bool is_admin_command(uint8_t opcode) {
    return opcode == CREATE_BUCKET
        || opcode == DELETE_BUCKET
        || opcode == LIST_BUCKETS
        || opcode == EXPAND_BUCKET
        || opcode == SELECT_BUCKET;
}

static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response)
{
    if (is_admin_command(request->request.opcode)
        && !authorized(handle, cookie)) {
        return ENGINE_ENOTSUP;
    }

    ENGINE_ERROR_CODE rv = ENGINE_ENOTSUP;
    switch(request->request.opcode) {
    case CREATE_BUCKET:
        rv = handle_create_bucket(handle, cookie, request, response);
        break;
    case DELETE_BUCKET:
        rv = handle_delete_bucket(handle, cookie, request, response);
        break;
    case LIST_BUCKETS:
        rv = handle_list_buckets(handle, cookie, request, response);
        break;
    case EXPAND_BUCKET:
        rv = handle_expand_bucket(handle, cookie, request, response);
        break;
    case SELECT_BUCKET:
        rv = handle_select_bucket(handle, cookie, request, response);
        break;
    default: {
        proxied_engine_t *e = get_engine(handle, cookie);
        if (e) {
            rv = e->v1->unknown_command(e->v0, cookie, request, response);
        } else {
            rv = ENGINE_DISCONNECT;
        }
    }
    }
    return rv;
}
