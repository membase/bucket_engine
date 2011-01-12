/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <stddef.h>

#include <memcached/engine.h>
#include <memcached/genhash.h>

#include "bucket_engine.h"

typedef union proxied_engine {
    ENGINE_HANDLE    *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

typedef enum {
    STATE_NULL,
    STATE_RUNNING,
    STATE_STOPPING
} bucket_state_t;

typedef struct proxied_engine_handle {
    const char          *name;
    size_t               name_len;
    proxied_engine_t     pe;
    struct thread_stats *stats;
    TAP_ITERATOR         tap_iterator;
    /* ON_DISCONNECT handling */
    bool                 wants_disconnects;
    /* Force shutdown flag */
    bool                 force_shutdown;
    EVENT_CALLBACK       cb;
    const void          *cb_data;
    pthread_mutex_t      lock; /* guards everything below */
    int                  refcount; /* count of connections + 1 for
                                    * hashtable reference. Handle
                                    * itself can be freed when this
                                    * drops to zero. This can only
                                    * happen when bucket is deleted
                                    * (but can happen later because
                                    * some connection can hold
                                    * pointer longer) */
    volatile bucket_state_t state;
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
    pthread_mutex_t dlopen_mutex;
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

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const engine_info* bucket_get_info(ENGINE_HANDLE* handle);

static const char *get_default_bucket_config(void);

static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str);
static void bucket_destroy(ENGINE_HANDLE* handle,
                           const bool force);
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

static bool authorized(ENGINE_HANDLE* handle, const void* cookie);

static void free_engine_handle(proxied_engine_handle_t *);

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
            {.feature = ENGINE_FEATURE_MULTI_TENANCY,
             .description = "Multi tenancy"}
        }
    },
};

/* Internal utility functions */

static void must_lock(pthread_mutex_t *mutex)
{
    int rv = pthread_mutex_lock(mutex);
    assert(rv == 0);
}

static void must_unlock(pthread_mutex_t *mutex)
{
    int rv = pthread_mutex_unlock(mutex);
    assert(rv == 0);
}

static void lock_engines(void)
{
    must_lock(&bucket_engine.engines_mutex);
}

static void unlock_engines(void)
{
    must_unlock(&bucket_engine.engines_mutex);
}

static const char * bucket_state_name(bucket_state_t s) {
    const char * rv = NULL;
    switch(s) {
    case STATE_NULL: rv = "NULL"; break;
    case STATE_RUNNING: rv = "running"; break;
    case STATE_STOPPING: rv = "stopping"; break;
    }
    assert(rv);
    return rv;
}

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
    (void)key;
    (void)nkey;
    (void)nval;
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
    /* This is called from underlying engine 'initialize' handler
     * which we invoke with engines_mutex held */

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
    (void)type;
    (void)data;
    (void)cookie;
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
    (void)type;
    (void)extension;
    return false;
}

static void bucket_unregister_extension(extension_type_t type, void *extension) {
    (void)type;
    (void)extension;
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

static void *engine_destroyer(void *arg) {
    proxied_engine_handle_t *peh = arg;
    assert(peh->state == STATE_STOPPING);
    assert(peh->refcount == 0);
    /* at this point we know that no connections holds reference to
     * this bucket. The only place that still holds reference to that
     * bucket is hashtable. But find_bucket will not return it because
     * it's marked as STATE_STOPPING. So it's safe to destroy it */
    peh->pe.v1->destroy(peh->pe.v0, peh->force_shutdown);

    /* now we can delete it from hashtable */
    lock_engines();

    int upd = genhash_delete_all(bucket_engine.engines,
                                 peh->name, peh->name_len);
    assert(upd == 1);
    assert(genhash_find(bucket_engine.engines,
                        peh->name, peh->name_len) == NULL);
    assert(peh->state == STATE_NULL);

    unlock_engines();

    /* to make sure release_handle unlocked peh before we free it, we
     * briefly acquire lock again */
    must_lock(&peh->lock);
    must_unlock(&peh->lock);

    /* and free it */
    free_engine_handle(peh);

    return NULL;
}

static void release_handle_locked(proxied_engine_handle_t *peh) {
    assert(peh->refcount > 0);
    if (--peh->refcount == 0) {
        assert(peh->state == STATE_STOPPING);

        // We should never free the default engine.
        assert(peh != &bucket_engine.default_engine);

        pthread_attr_t attr;
        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0) {
            abort();
        }

        pthread_t tid;
        if (pthread_create(&tid, &attr, engine_destroyer, peh) != 0) {
            abort();
        }
        pthread_attr_destroy(&attr);
    }
}

static void release_handle(proxied_engine_handle_t *peh) {
    if (!peh) {
        return;
    }

    must_lock(&peh->lock);
    release_handle_locked(peh);
    must_unlock(&peh->lock);
}

static proxied_engine_handle_t *find_bucket_inner(const char *name) {
    return genhash_find(bucket_engine.engines, name, strlen(name));
}

/* returns proxied_engine_handle_t for bucket with given name. This
 * increments refcount of returned handle, so caller is responsible
 * for calling release_handle on it. */
static proxied_engine_handle_t *find_bucket(const char *name) {
    lock_engines();
    proxied_engine_handle_t *rv = find_bucket_inner(name);
    if (rv) {
        pthread_mutex_t *lock = &rv->lock;
        must_lock(lock);
        if (rv->state == STATE_RUNNING) {
            rv->refcount++;
        } else {
            rv = NULL;
        }
        must_unlock(lock);
    }
    unlock_engines();
    return rv;
}

static proxied_engine_handle_t* retain_handle(proxied_engine_handle_t *peh) {
    proxied_engine_handle_t *rv = NULL;
    if (peh) {
        must_lock(&peh->lock);
        if (peh->state == STATE_RUNNING) {
            ++peh->refcount;
            assert(peh->refcount > 0);
            rv = peh;
        }
        must_unlock(&peh->lock);
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

/* fills engine handle. Assumes that it's zeroed already */
static void init_engine_handle(proxied_engine_handle_t *peh, const char *name) {
    peh->stats = bucket_engine.upstream_server->stat->new_stats();
    assert(peh->stats);
    peh->refcount = 1;
    peh->name = strdup(name);
    peh->name_len = strlen(peh->name);
    pthread_mutex_init(&peh->lock, NULL);
    peh->state = STATE_RUNNING;
}

static void free_engine_handle(proxied_engine_handle_t *peh) {
    pthread_mutex_destroy(&peh->lock);
    bucket_engine.upstream_server->stat->release_stats(peh->stats);
    free((void *)peh->name);
    free(peh);
}

/* Creates bucket and places it's handle into *e_out. NOTE: that
 * caller is responsible for calling release_handle on that handle */
static ENGINE_ERROR_CODE create_bucket(struct bucket_engine *e,
                                       const char *bucket_name,
                                       const char *path,
                                       const char *config,
                                       proxied_engine_handle_t **e_out,
                                       char *msg, size_t msglen) {

    if (!has_valid_bucket_name(bucket_name)) {
        return ENGINE_EINVAL;
    }

    proxied_engine_handle_t *peh = calloc(sizeof(proxied_engine_handle_t), 1);
    if (peh == NULL) {
        return ENGINE_FAILED;
    }
    init_engine_handle(peh, bucket_name);

    ENGINE_ERROR_CODE rv = ENGINE_FAILED;

    must_lock(&bucket_engine.dlopen_mutex);
    peh->pe.v0 = load_engine(path, NULL, NULL);
    must_unlock(&bucket_engine.dlopen_mutex);

    if (!peh->pe.v0) {
        free_engine_handle(peh);
        if (msg) {
            snprintf(msg, msglen, "Failed to load engine.");
        }
        return rv;
    }

    lock_engines();

    proxied_engine_handle_t *tmppeh = find_bucket_inner(bucket_name);
    if (tmppeh == NULL) {
        genhash_update(e->engines, bucket_name, strlen(bucket_name), peh, 0);

        // This was already verified, but we'll check it anyway
        assert(peh->pe.v0->interface == 1);

        rv = ENGINE_SUCCESS;

        if (peh->pe.v1->initialize(peh->pe.v0, config) != ENGINE_SUCCESS) {
            peh->pe.v1->destroy(peh->pe.v0, false);
            genhash_delete_all(e->engines, bucket_name, strlen(bucket_name));
            if (msg) {
                snprintf(msg, msglen,
                         "Failed to initialize instance. Error code: %d\n", rv);
            }
            rv = ENGINE_FAILED;
        }
    } else {
        if (msg) {
            snprintf(msg, msglen,
                     "Bucket exists: %s", bucket_state_name(tmppeh->state));
        }
        rv = ENGINE_KEY_EEXISTS;
    }

    unlock_engines();

    if (rv == ENGINE_SUCCESS) {
        if (e_out) {
            *e_out = peh;
        } else {
            release_handle(peh);
        }
    } else {
        free_engine_handle(peh);
    }

    return rv;
}

/* Returns engine handle for this connection. Every access to
 * underlying engine must go through this function. */
static proxied_engine_handle_t *get_engine_handle(ENGINE_HANDLE *h,
                                                   const void *cookie) {
    struct bucket_engine *e = (struct bucket_engine*)h;
    engine_specific_t *es = e->upstream_server->cookie->get_engine_specific(cookie);
    if (!es) {
        return NULL;
    }
    proxied_engine_handle_t *peh = es->peh;

    if (!peh) {
        return e->default_engine.pe.v0 ? &e->default_engine : NULL;
    }

    if (peh->state != STATE_RUNNING) {
        release_handle(es->peh);
        e->upstream_server->cookie->store_engine_specific(cookie, NULL);
        free(es);
        return NULL;
    }

    return peh;
}

static proxied_engine_handle_t* set_engine_handle(ENGINE_HANDLE *h,
                                                  const void *cookie,
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
    return es->peh;
}

static proxied_engine_t *get_engine(ENGINE_HANDLE *h,
                                     const void *cookie) {
    proxied_engine_handle_t *peh = get_engine_handle(h, cookie);

    if (!peh)
        return NULL;

    return &peh->pe;
}

static proxied_engine_handle_t *engine_to_engine_handle(proxied_engine_t *e) {
    return (proxied_engine_handle_t *)(((char *)e) - offsetof(proxied_engine_handle_t, pe));
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
    (void)vlen;
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    assert(peh);
    must_lock(&peh->lock);
    peh->refcount++;
    must_unlock(&peh->lock);
    return (void*)ob;
}

static void engine_hash_free(void* ob) {
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    assert(peh);
    peh->state = STATE_NULL;
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
                v1->destroy(engine, false);
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
    (void)type;
    (void)event_data;
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    proxied_engine_handle_t *peh = NULL;
    if (e->default_bucket_name != NULL) {
        // Assign a default named bucket (if there is one).
        peh = find_bucket(e->default_bucket_name);
        if (!peh && e->auto_create) {
            // XXX:  Need default config.
            create_bucket(e, e->default_bucket_name,
                          e->default_engine_path,
                          get_default_bucket_config(), &peh, NULL, 0);
        }
    } else {
        // Assign the default bucket (if there is one).
        peh = e->default_engine.pe.v0 ? &e->default_engine : NULL;
        if (peh != NULL) {
            /* increment refcount because final release_handle will
             * decrement it */
            proxied_engine_handle_t *t = retain_handle(peh);
            assert(t == peh);
        }
    }

    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
    release_handle(peh);
}

static void handle_auth(const void *cookie,
                        ENGINE_EVENT_TYPE type,
                        const void *event_data,
                        const void *cb_data) {
    (void)type;
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    const auth_data_t *auth_data = (const auth_data_t*)event_data;
    proxied_engine_handle_t *peh = find_bucket(auth_data->username);
    if (!peh && e->auto_create) {
        create_bucket(e, auth_data->username, e->default_engine_path,
                      auth_data->config ? auth_data->config : "", &peh, NULL, 0);
    }
    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
    release_handle(peh);
}

static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str) {
    struct bucket_engine* se = get_handle(handle);

    assert(!se->initialized);

    if (pthread_mutex_init(&se->engines_mutex, NULL) != 0) {
        fprintf(stderr, "Error initializing mutex for bucket engine.\n");
        return ENGINE_FAILED;
    }

    if (pthread_mutex_init(&se->dlopen_mutex, NULL) != 0) {
        fprintf(stderr, "Error initializing mutex for bucket engine dlopen.\n");
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
        init_engine_handle(&se->default_engine, "");
        se->default_engine.pe.v0 = load_engine(se->default_engine_path, NULL, NULL);

        ENGINE_HANDLE_V1 *dv1 = (ENGINE_HANDLE_V1*)se->default_engine.pe.v0;
        if (!dv1) {
            return ENGINE_FAILED;
        }

        if (dv1->initialize(se->default_engine.pe.v0, config_str) != ENGINE_SUCCESS) {
            dv1->destroy(se->default_engine.pe.v0, false);
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

static void bucket_destroy(ENGINE_HANDLE* handle,
                           const bool force) {
    (void)force;
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
        pthread_mutex_destroy(&se->dlopen_mutex);
        se->initialized = false;
    }
}

static ENGINE_ERROR_CODE bucket_item_allocate(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              item **itm,
                                              const void* key,
                                              const size_t nkey,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->allocate(e->v0, cookie, itm, key,
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
                                item* itm) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->release(e->v0, cookie, itm);
    }
}

static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** itm,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get(e->v0, cookie, itm, key, nkey, vbucket);
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
    (void)nval;
    struct bucket_list **blist_ptr = (struct bucket_list **)arg;
    struct bucket_list *n = calloc(sizeof(struct bucket_list), 1);
    n->name = (char*)key;
    n->namelen = nkey;
    n->peh = (proxied_engine_handle_t*) val;
    assert(n->peh);

    /* we must not leak dead buckets outside of engines_mutex. Those
     * can be freed by bucket destructor at any time (when
     * engines_mutex is not held) */
    if (retain_handle(n->peh) == NULL) {
        free(n);
        return;
    }

    n->next = *blist_ptr;
    *blist_ptr = n;
}

static bool list_buckets(struct bucket_engine *e, struct bucket_list **blist) {
    lock_engines();
    genhash_iter(e->engines, add_engine, blist);
    unlock_engines();
    return true;
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
    (void)cookie;
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

struct stat_context {
    ADD_STAT add_stat;
    const void *cookie;
};

static void stat_ht_builder(const void *key, size_t nkey,
                            const void *val, size_t nval,
                            void *arg) {
    (void)nval;
    assert(arg);
    struct stat_context *ctx = (struct stat_context*)arg;
    proxied_engine_handle_t *bucket = (proxied_engine_handle_t*)val;
    const char * const bucketState = bucket_state_name(bucket->state);
    ctx->add_stat(key, nkey, bucketState, strlen(bucketState),
                  ctx->cookie);
}

static ENGINE_ERROR_CODE get_bucket_stats(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          ADD_STAT add_stat) {

    if (!authorized(handle, cookie)) {
        return ENGINE_FAILED;
    }

    struct bucket_engine *e = (struct bucket_engine*)handle;
    struct stat_context sctx = {.add_stat = add_stat, .cookie = cookie};

    lock_engines();
    genhash_iter(e->engines, stat_ht_builder, &sctx);
    unlock_engines();
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const char* stat_key,
                                          int nkey,
                                          ADD_STAT add_stat) {
    // Intercept bucket stats.
    if (nkey == strlen("bucket") && memcmp("bucket", stat_key, nkey) == 0) {
        return get_bucket_stats(handle, cookie, add_stat);
    }

    ENGINE_ERROR_CODE rc = ENGINE_DISCONNECT;
    proxied_engine_t *e = get_engine(handle, cookie);

    if (e) {
        rc = e->v1->get_stats(e->v0, cookie, stat_key, nkey, add_stat);
        proxied_engine_handle_t *peh = engine_to_engine_handle(e);
        if (nkey == 0) {
            char statval[20];
            snprintf(statval, sizeof(statval), "%d", peh->refcount - 1);
            add_stat("bucket_conns", strlen("bucket_conns"), statval,
                     strlen(statval), cookie);
        }
    }
    return rc;
}

static void *bucket_get_stats_struct(ENGINE_HANDLE* handle,
                                     const void* cookie) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    void *rv =  NULL;
    if (peh != NULL) {
        rv = peh->stats;
    }
    return rv;
}

static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* itm,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->store(e->v0, cookie, itm, cas, operation, vbucket);
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
                                 const item* itm,
                                 item_info *itm_info) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        return e->v1->get_item_info(e->v0, cookie, itm, itm_info);
    } else {
        return false;
    }
}

static void bucket_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item *itm, uint64_t cas) {
    proxied_engine_t *e = get_engine(handle, cookie);
    if (e) {
        e->v1->item_set_cas(e->v0, cookie, itm, cas);
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
                                            item **itm,
                                            void **engine_specific,
                                            uint16_t *nengine_specific,
                                            uint8_t *ttl,
                                            uint16_t *flags,
                                            uint32_t *seqno,
                                            uint16_t *vbucket) {
    proxied_engine_handle_t *e = get_engine_handle(handle, cookie);
    if (e && e->tap_iterator) {
        assert(e->pe.v0 != handle);
        return e->tap_iterator(e->pe.v0, cookie, itm,
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

    if (bodylen >= (1 << 16)) // 64k ought to be enough for anybody
        return ENGINE_DISCONNECT;

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

    const size_t msglen = 1024;
    char msg[msglen];
    ENGINE_ERROR_CODE ret = create_bucket(e, keyz, spec,
                                          config ? config : "",
                                          NULL, msg, msglen);

    protocol_binary_response_status rc = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch(ret) {
    case ENGINE_SUCCESS:
        // Defaults as above.
        break;
    case ENGINE_KEY_EEXISTS:
        rc = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        break;
    default:
        rc = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    }

    response(NULL, 0, NULL, 0, msg, strlen(msg), 0, rc, 0, cookie);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE handle_delete_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    (void)handle;
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    size_t bodylen = ntohl(breq->message.header.request.bodylen)
                     - ntohs(breq->message.header.request.keylen);
    if (bodylen >= (1 << 16)) {
        return ENGINE_DISCONNECT;
    }
    char config[bodylen + 1];
    memcpy(config, ((char*)request) + sizeof(breq->message.header)
           + ntohs(breq->message.header.request.keylen), bodylen);
    config[bodylen] = 0x00;

    bool force = false;
    if (config[0] != 0) {
        struct config_item items[2] = {
            {.key = "force",
             .datatype = DT_BOOL,
             .value.dt_bool = &force},
            {.key = NULL}
        };

        if (bucket_get_server_api()->core->parse_config(config, items, stderr) != 0) {
            const char *msg = "Invalid config parameters";
            response(msg, strlen(msg), "", 0, "", 0, 0,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
            return ENGINE_SUCCESS;
        }
    }

    bool found = false;
    proxied_engine_handle_t *peh = find_bucket(keyz);

    if (peh) {
        must_lock(&peh->lock);
        if (peh->state == STATE_RUNNING) {
            found = true;
            peh->state = STATE_STOPPING;
            peh->force_shutdown = force;
            /* now drop main ref */
            release_handle_locked(peh);
        }
        must_unlock(&peh->lock);
    }
    release_handle(peh);

    if (found) {
        response("", 0, "", 0, "", 0, 0, 0, 0, cookie);
    } else {
        const char *msg = "Not found.";
        response(NULL, 0, NULL, 0, msg, strlen(msg),
                 0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                 0, cookie);
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE handle_list_buckets(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response) {
    (void)request;
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
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    proxied_engine_handle_t *proxied = find_bucket(keyz);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    if (proxied) {
        rv = proxied->pe.v1->unknown_command(handle, cookie, request, response);
        release_handle(proxied);
    } else {
        const char *msg = "Engine not found";
        response(NULL, 0, NULL, 0, msg, strlen(msg),
                 0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                 0, cookie);
    }

    return rv;
}

static ENGINE_ERROR_CODE handle_select_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    protocol_binary_request_delete_bucket *breq =
        (protocol_binary_request_delete_bucket*)request;

    EXTRACT_KEY(breq, keyz);

    proxied_engine_handle_t *proxied = find_bucket(keyz);
    set_engine_handle(handle, cookie, proxied);
    release_handle(proxied);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    if (proxied) {
        response("", 0, "", 0, "", 0, 0, 0, 0, cookie);
    } else {
        const char *msg = "Engine not found";
        response(NULL, 0, NULL, 0, msg, strlen(msg),
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
