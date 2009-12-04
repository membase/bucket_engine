#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <memcached/engine.h>

#include "genhash.h"

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
    bool initialized;
    genhash_t *hashtbl;
    struct mock_stats stats;
    uint64_t magic2;
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const char* mock_get_info(ENGINE_HANDLE* handle);
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
                                          item* item);
static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* item);
static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey);
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
                                    ENGINE_STORE_OPERATION operation);
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
                                         uint64_t *result);
static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when);
static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);
static char* item_get_data(const item* item);
static char* item_get_key(const item* item);
static void item_set_cas(item* item, uint64_t val);
static uint64_t item_get_cas(const item* item);
static uint8_t item_get_clsid(const item* item);

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
    h->engine.item_get_cas = item_get_cas;
    h->engine.item_set_cas = item_set_cas;
    h->engine.item_get_key = item_get_key;
    h->engine.item_get_data = item_get_data;
    h->engine.item_get_clsid = item_get_clsid;

    h->magic = MAGIC;
    h->magic2 = MAGIC;

    *handle = (ENGINE_HANDLE *)h;

    return ENGINE_SUCCESS;
}

static inline struct mock_engine* get_handle(ENGINE_HANDLE* handle) {
    struct mock_engine *e = (struct mock_engine*)handle;
    assert(e->magic == MAGIC);
    assert(e->magic2 == MAGIC);
    return e;
}

static const char* mock_get_info(ENGINE_HANDLE* handle) {
    return "Mock engine v0.1";
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

    se->initialized = true;

    return ENGINE_SUCCESS;
}

static void mock_destroy(ENGINE_HANDLE* handle) {
    struct mock_engine* se = get_handle(handle);

    if (se->initialized) {
        se->initialized = false;
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
        size_t to_alloc = sizeof(item) + nkey + nbytes;
        *it = calloc(to_alloc, 1);
    } else {
        *it = NULL;
    }
    // If an allocation was requested *and* worked, fill and report success
    if (*it) {
        item *i = *it;
        i->exptime = exptime;
        i->nbytes = nbytes;
        i->flags = flags;
        i->nkey = nkey;
        memcpy(item_get_key(i), key, nkey);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE mock_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          item* item) {
    genhash_delete_all(get_ht(handle), item_get_key(item), item->nkey);
    return ENGINE_SUCCESS;
}

static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* item) {
    free(item);
}

static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey) {
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
                                    ENGINE_STORE_OPERATION operation) {
    genhash_update(get_ht(handle), item_get_key(item), item->nkey, item, 0);
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
                                         uint64_t *result) {
    item *item_in = NULL, *item_out = NULL;
    int flags = 0;
    *cas = 0;

    if (mock_get(handle, cookie, &item_in, key, nkey) == ENGINE_SUCCESS) {
        // Found, just do the math.
        // This is all int stuff, just to make it easy.
        *result = atoi(item_get_data(item_in));
        *result += delta;
        flags = item_in->flags;
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
                                strlen(buf),
                                flags, exptime)) != ENGINE_SUCCESS) {
        return rv;
    }
    memcpy(item_get_data(item_out), buf, strlen(buf));
    mock_store(handle, cookie, item_out, 0, OPERATION_SET);
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
    return ENGINE_ENOTSUP;
}

static uint64_t item_get_cas(const item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

static void item_set_cas(item* item, uint64_t val)
{
    if (item->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(item + 1) = val;
    }
}

static char* item_get_key(const item* item)
{
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }

    return ret;
}

static char* item_get_data(const item* item)
{
    return item_get_key(item) + item->nkey + 1;
}

static uint8_t item_get_clsid(const item* item)
{
    return 0;
}
