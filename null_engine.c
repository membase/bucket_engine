#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <memcached/engine.h>

typedef union proxied_engine {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

struct null_engine {
    ENGINE_HANDLE_V1 engine;
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const char* null_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE null_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str);
static void null_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE null_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **item,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime);
static ENGINE_ERROR_CODE null_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          item* item);
static void null_item_release(ENGINE_HANDLE* handle,
                              const void *cookie,
                              item* item);
static ENGINE_ERROR_CODE null_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey);
static ENGINE_ERROR_CODE null_get_stats(ENGINE_HANDLE* handle,
                                        const void *cookie,
                                        const char *stat_key,
                                        int nkey,
                                        ADD_STAT add_stat);
static void null_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE null_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation);
static ENGINE_ERROR_CODE null_arithmetic(ENGINE_HANDLE* handle,
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
static ENGINE_ERROR_CODE null_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when);
static ENGINE_ERROR_CODE null_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);
static char* item_get_data(const item* item);
static char* item_get_key(const item* item);
static void item_set_cas(item* item, uint64_t val);
static uint64_t item_get_cas(const item* item);
static uint8_t item_get_clsid(const item* item);

struct null_engine null_engine = {
    .engine = {
        .interface = {
            .interface = 1
        },
        .get_info = null_get_info,
        .initialize = null_initialize,
        .destroy = null_destroy,
        .allocate = null_item_allocate,
        .remove = null_item_delete,
        .release = null_item_release,
        .get = null_get,
        .get_stats = null_get_stats,
        .reset_stats = null_reset_stats,
        .store = null_store,
        .arithmetic = null_arithmetic,
        .flush = null_flush,
        .unknown_command = null_unknown_command,
        .item_get_cas = item_get_cas,
        .item_set_cas = item_set_cas,
        .item_get_key = item_get_key,
        .item_get_data = item_get_data,
        .item_get_clsid = item_get_clsid
    },
};

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    *handle = (ENGINE_HANDLE*)&null_engine;
    return ENGINE_SUCCESS;
}

static inline struct null_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct null_engine*)handle;
}

static const char* null_get_info(ENGINE_HANDLE* handle) {
    return "Null engine v0.1";
}

static ENGINE_ERROR_CODE null_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str) {
    return ENGINE_SUCCESS;
}

static void null_destroy(ENGINE_HANDLE* handle) {
}

static ENGINE_ERROR_CODE null_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **item,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime) {
    return ENGINE_ENOMEM;
}

static ENGINE_ERROR_CODE null_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          item* item) {
    return ENGINE_SUCCESS;
}

static void null_item_release(ENGINE_HANDLE* handle,
                              const void *cookie,
                              item* item) {
    /* nothing should ever call this */
    assert(false);
}

static ENGINE_ERROR_CODE null_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey) {
    return ENGINE_KEY_ENOENT;
}

static ENGINE_ERROR_CODE null_get_stats(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const char* stat_key,
                                        int nkey,
                                        ADD_STAT add_stat)
{
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE null_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation) {
    return ENGINE_NOT_STORED;
}

static ENGINE_ERROR_CODE null_arithmetic(ENGINE_HANDLE* handle,
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
    return ENGINE_NOT_STORED;
}

static ENGINE_ERROR_CODE null_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when) {
    return ENGINE_SUCCESS;
}

static void null_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
}

static ENGINE_ERROR_CODE null_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response)
{
    return ENGINE_ENOTSUP;
}

static char* item_get_data(const item* item) {
    assert(false);
    return NULL;
}

static char* item_get_key(const item* item) {
    assert(false);
    return NULL;
}

static void item_set_cas(item* item, uint64_t val) {
    assert(false);
}

static uint64_t item_get_cas(const item* item) {
    assert(false);
    return 0;
}

static uint8_t item_get_clsid(const item* item) {
    assert(false);
    return 0;
}
