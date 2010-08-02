#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <memcached/engine.h>
#include <memcached/genhash.h>

#include "bucket_engine.h"

#define MAGIC 0x426D4639C1BFEC3ll

#define ITEM_LINKED 1
#define ITEM_WITH_CAS 2

struct mock_stats {
    int get_reqs;
    int set_reqs;
    int current;
};

struct mock_engine {
    ENGINE_HANDLE_V1 engine;
    uint64_t magic;
    SERVER_HANDLE_V1 *server;
    bool initialized;
    genhash_t *hashtbl;
    struct mock_stats stats;
    int disconnects;
    uint64_t magic2;

    union {
      engine_info engine_info;
      char buffer[sizeof(engine_info) +
                  (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;
};

typedef struct mock_item {
    uint64_t cas;
    rel_time_t exptime; /**< When the item will expire (relative to process
                             * startup) */
    uint32_t nbytes; /**< The total size of the data (in bytes) */
    uint32_t flags; /**< Flags associated with the item (in network byte order)*/
    uint8_t clsid; /** class id for the object */
    uint16_t nkey; /**< The total length of the key (in bytes) */
} mock_item;

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const engine_info* mock_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE mock_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str);
static void mock_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE mock_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **item,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime);
static ENGINE_ERROR_CODE mock_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* key,
                                          const size_t nkey,
                                          uint64_t cas,
                                          uint16_t vbucket);
static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* item);
static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey,
                                  uint16_t vbucket);
static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void *cookie,
                                        const char *stat_key,
                                        int nkey,
                                        ADD_STAT add_stat);
static void mock_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket);
static ENGINE_ERROR_CODE mock_arithmetic(ENGINE_HANDLE* handle,
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
static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when);
static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);
static char* item_get_data(const item* item);
static const char* item_get_key(const item* item);
static void item_set_cas(ENGINE_HANDLE* handle, const void *cookie,
                         item* item, uint64_t val);
static uint64_t item_get_cas(const item* item);
static uint8_t item_get_clsid(const item* item);

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info);

static void handle_disconnect(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data) {
    assert(type == ON_DISCONNECT);
    struct mock_engine *h = (struct mock_engine*)cb_data;
    ++h->disconnects;
}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    struct mock_engine *h = calloc(sizeof(struct mock_engine), 1);
    assert(h);
    h->engine.interface.interface = 1;
    h->engine.get_info = mock_get_info;
    h->engine.initialize = mock_initialize;
    h->engine.destroy = mock_destroy;
    h->engine.allocate = mock_item_allocate;
    h->engine.remove = mock_item_delete;
    h->engine.release = mock_item_release;
    h->engine.get = mock_get;
    h->engine.get_stats = mock_get_stats;
    h->engine.reset_stats = mock_reset_stats;
    h->engine.store = mock_store;
    h->engine.arithmetic = mock_arithmetic;
    h->engine.flush = mock_flush;
    h->engine.unknown_command = mock_unknown_command;
    h->engine.item_set_cas = item_set_cas;
    h->engine.get_item_info = get_item_info;

    h->server = gsapi();

    h->magic = MAGIC;
    h->magic2 = MAGIC;

    h->info.engine_info.description = "Mock engine v0.2";
    h->info.engine_info.num_features = 0;

    *handle = (ENGINE_HANDLE *)h;

    return ENGINE_SUCCESS;
}

static inline struct mock_engine* get_handle(ENGINE_HANDLE* handle) {
    struct mock_engine *e = (struct mock_engine*)handle;
    assert(e->magic == MAGIC);
    assert(e->magic2 == MAGIC);
    return e;
}

static const engine_info* mock_get_info(ENGINE_HANDLE* handle) {
    return &get_handle(handle)->info.engine_info;
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

static void noop_free(void* ob) {
    // Nothing
}

static struct hash_ops my_hash_ops = {
    .hashfunc = genhash_string_hash,
    .hasheq = my_hash_eq,
    .dupKey = hash_strdup,
    .dupValue = noop_dup,
    .freeKey = free,
    .freeValue = noop_free
};

static ENGINE_ERROR_CODE mock_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str) {
    struct mock_engine* se = get_handle(handle);

    assert(my_hash_ops.dupKey);

    if (strcmp(config_str, "no_alloc") != 0) {
        se->hashtbl = genhash_init(1, my_hash_ops);
        assert(se->hashtbl);
    }

    se->server->callback->register_callback((ENGINE_HANDLE*)se, ON_DISCONNECT,
                                            handle_disconnect, se);

    se->initialized = true;

    return ENGINE_SUCCESS;
}

static void mock_destroy(ENGINE_HANDLE* handle) {
    struct mock_engine* se = get_handle(handle);

    if (se->initialized) {
        se->initialized = false;
        genhash_free(se->hashtbl);
        free(se);
    }
}

static genhash_t *get_ht(ENGINE_HANDLE *handle) {
    return get_handle(handle)->hashtbl;
}

static ENGINE_ERROR_CODE mock_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **it,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime) {

    // Only perform allocations if there's a hashtable.
    if (get_ht(handle) != NULL) {
        size_t to_alloc = sizeof(mock_item) + nkey + nbytes;
        *it = calloc(to_alloc, 1);
    } else {
        *it = NULL;
    }
    // If an allocation was requested *and* worked, fill and report success
    if (*it) {
        mock_item* i = (mock_item*) *it;
        i->exptime = exptime;
        i->nbytes = nbytes;
        i->flags = flags;
        i->nkey = nkey;
        memcpy((char*)item_get_key(i), key, nkey);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE mock_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* key,
                                          const size_t nkey,
                                          uint64_t cas,
                                          uint16_t vbucket) {
    int r = genhash_delete_all(get_ht(handle), key, nkey);
    return r > 0 ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT;
}

static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* item) {
    free(item);
}

static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey,
                                  uint16_t vbucket) {
    *item = genhash_find(get_ht(handle), key, nkey);

    return *item ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT;
}

static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const char* stat_key,
                                        int nkey,
                                        ADD_STAT add_stat)
{
    // TODO:  Implement
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket) {
    mock_item* it = (mock_item*)item;
    genhash_update(get_ht(handle), item_get_key(item), it->nkey, item, 0);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_arithmetic(ENGINE_HANDLE* handle,
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
    item *item_in = NULL, *item_out = NULL;
    int flags = 0;
    *cas = 0;

    if (mock_get(handle, cookie, &item_in, key, nkey, 0) == ENGINE_SUCCESS) {
        // Found, just do the math.
        // This is all int stuff, just to make it easy.
        *result = atoi(item_get_data(item_in));
        *result += delta;
        flags = ((mock_item*) item_in)->flags;
    } else if (create) {
        // Not found, do the initialization
        *result = initial;
    } else {
        // Reject.
        return ENGINE_KEY_ENOENT;
    }

    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", *result);
    ENGINE_ERROR_CODE rv;
    if((rv = mock_item_allocate(handle, cookie, &item_out,
                                key, nkey,
                                strlen(buf) + 1,
                                flags, exptime)) != ENGINE_SUCCESS) {
        return rv;
    }
    memcpy(item_get_data(item_out), buf, strlen(buf) + 1);
    mock_store(handle, cookie, item_out, 0, OPERATION_SET, 0);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when) {
    genhash_clear(get_ht(handle));
    return ENGINE_SUCCESS;
}

static void mock_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    // TODO:  Implement
}

static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response)
{
    if (request->request.opcode != EXPAND_BUCKET) {
        return ENGINE_ENOTSUP;
    }

    response("", 0, "", 0, "", 0, 0, 0, 0, cookie);
    return ENGINE_SUCCESS;
}

static uint64_t item_get_cas(const item* item)
{
    const mock_item* it = (mock_item*)item;
    return it->cas;
}

static void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                         item* item, uint64_t val)
{
    mock_item* it = (mock_item*)item;
    it->cas = val;
}

static const char* item_get_key(const item* item)
{
    const mock_item* it = (mock_item*)item;
    char *ret = (void*)(it + 1);
    return ret;
}

static char* item_get_data(const item* item)
{
    const mock_item* it = (mock_item*)item;
    return ((char*)item_get_key(item)) + it->nkey;
}

static uint8_t item_get_clsid(const item* item)
{
    return 0;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    mock_item* it = (mock_item*)item;
    if (item_info->nvalue < 1) {
        return false;
    }
    item_info->cas = item_get_cas(it);
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->clsid = it->clsid;
    item_info->nkey = it->nkey;
    item_info->nvalue = 1;
    item_info->key = item_get_key(it);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    return true;
}
