#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dlfcn.h>
#include <arpa/inet.h>
#include <assert.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

#include "bucket_engine.h"

#include "memcached/engine.h"

#define DEFAULT_CONFIG "engine=.libs/mock_engine.so;default=true;admin=admin" \
    ";auto_create=false"
#define DEFAULT_CONFIG_NO_DEF "engine=.libs/mock_engine.so;default=false;admin=admin" \
    ";auto_create=false"
#define DEFAULT_CONFIG_AC "engine=.libs/mock_engine.so;default=true;admin=admin" \
    ";auto_create=true"

#define CONN_MAGIC 16369814453946373207ULL

protocol_binary_response_status last_status = 0;
char *last_key = NULL;
char *last_body = NULL;

enum test_result {
    SUCCESS = 11,
    FAIL    = 13,
    DIED    = 14,
    CORE    = 15,
    PENDING = 19
};

struct test {
    const char *name;
    enum test_result (*tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
    const char *cfg;
};

struct connstruct {
    uint64_t magic;
    const char *uname;
    void *engine_data;
    bool connected;
    struct connstruct *next;
};

struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct engine_event_handler *next;
};

static struct connstruct *connstructs;

static struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

static inline void perform_callbacks(ENGINE_EVENT_TYPE type,
                                     const void *data,
                                     struct connstruct *c) {
    for (struct engine_event_handler *h = engine_event_handlers[type];
         h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}

static const char* get_server_version() {
    return "bucket mock";
}

static void get_auth_data(const void *cookie, auth_data_t *data) {
    struct connstruct *c = (struct connstruct *)cookie;
    if (c != NULL) {
        data->username = c->uname;
        data->config = "";
    }
}

static struct connstruct *mk_conn(const char *user) {
    struct connstruct *rv = calloc(sizeof(struct connstruct), 1);
    auth_data_t ad;
    assert(rv);
    rv->magic = CONN_MAGIC;
    rv->uname = user ? strdup(user) : NULL;
    rv->connected = true;
    rv->next = connstructs;
    connstructs = rv;
    perform_callbacks(ON_CONNECT, NULL, rv);
    if (rv->uname) {
        ad.username = rv->uname;
        ad.config = "";
        perform_callbacks(ON_AUTH, (const void*)&ad, rv);
    }
    return rv;
}

static void disconnect(struct connstruct *c) {
    c->connected = false;
    perform_callbacks(ON_DISCONNECT, NULL, c);
}

static void register_callback(ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data) {
    struct engine_event_handler *h =
        calloc(sizeof(struct engine_event_handler), 1);
    assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = engine_event_handlers[type];
    engine_event_handlers[type] = h;
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    if (cookie) {
        struct connstruct *c = (struct connstruct *)cookie;
        assert(c->magic == CONN_MAGIC);
        c->engine_data = engine_data;
    }
}

static void *get_engine_specific(const void *cookie) {
    struct connstruct *c = (struct connstruct *)cookie;
    assert(c == NULL || c->magic == CONN_MAGIC);
    return c ? c->engine_data : NULL;
}

static void *create_stats() {
    /* XXX: Not sure if ``big buffer'' is right in faking this part of
       the server. */
    void *s = calloc(1, 256);
    assert(s);
    return s;
}

static void destroy_stats(void *s) {
    assert(s);
    free(s);
}

/**
 * Callback the engines may call to get the public server interface
 * @param interface the requested interface from the server
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
static void *get_server_api(int interface)
{
    static struct server_interface_v1 server_api = {
        .register_callback = register_callback,
        .get_auth_data = get_auth_data,
        .server_version = get_server_version,
        .get_engine_specific = get_engine_specific,
        .store_engine_specific = store_engine_specific,
        .new_stats = create_stats,
        .release_stats = destroy_stats,
        .parse_config = parse_config,
    };

    if (interface != 1) {
        return NULL;
    }

    return &server_api;
}

bool add_response(const void *key, uint16_t keylen,
                  const void *ext, uint8_t extlen,
                  const void *body, uint32_t bodylen,
                  uint8_t datatype, uint16_t status,
                  uint64_t cas, const void *cookie) {
    last_status = status;
    if (last_body) {
        free(last_body);
        last_body = NULL;
    }
    if (bodylen > 0) {
        last_body = malloc(bodylen);
        assert(last_body);
        memcpy(last_body, body, bodylen);
    }
    if (last_key) {
        free(last_key);
        last_key = NULL;
    }
    if (keylen > 0) {
        last_key = malloc(keylen);
        assert(last_key);
        memcpy(last_key, key, keylen);
    }
    return true;
}

static ENGINE_HANDLE *load_engine(const char *soname, const char *config_str) {
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

    /* request a instance with protocol version 1 */
    ENGINE_ERROR_CODE error = (*my_create.create)(1, get_server_api, &engine);

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

// ----------------------------------------------------------------------
// The actual test stuff...
// ----------------------------------------------------------------------

static bool item_eq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                    item *i1, item *i2) {

    return i1->exptime == i2->exptime
        && i1->flags == i2->flags
        && i1->nkey == i2->nkey
        && i1->nbytes == i2->nbytes
        && memcmp(h1->item_get_key(i1),
                  h1->item_get_key(i2),
                  i1->nkey) == 0
        && memcmp(h1->item_get_data(i1),
                  h1->item_get_data(i2),
                  i1->nbytes) == 0;
}

static void assert_item_eq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           item *i1, item *i2) {
    assert(item_eq(h, h1, i1, i2));
}

/* Convenient storage abstraction */
static void store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                  const void *cookie,
                  const char *key, const char *value,
                  item **outitem) {

    item *item = NULL;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    memcpy(h1->item_get_data(item), value, strlen(value));

    rv = h1->store(h, cookie, item, 0, OPERATION_SET);
    assert(rv == ENGINE_SUCCESS);

    if (outitem) {
        *outitem = item;
    }
}

static enum test_result test_default_storage(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    item *item = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL);
    char *key = "somekey";
    char *value = "some value";

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    memcpy(h1->item_get_data(item), value, strlen(value));

    rv = h1->store(h, cookie, item, 0, OPERATION_SET);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, item, fetched_item);

    // no effect, but increases coverage.
    h1->reset_stats(h, cookie);

    return SUCCESS;
}

static enum test_result test_default_storage_key_overrun(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    item *item = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL);
    char *key = "somekeyx";
    char *value = "some value";

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key)-1,
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    memcpy(h1->item_get_data(item), value, strlen(value));

    rv = h1->store(h, cookie, item, 0, OPERATION_SET);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie, &fetched_item, "somekey", strlen("somekey"));
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, item, fetched_item);

    rv = h1->remove(h, cookie, fetched_item);
    assert(rv == ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_default_unlinked_remove(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    item *item = NULL;
    const void *cookie = mk_conn(NULL);
    char *key = "somekeyx";
    const char *value = "the value";

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key)-1,
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);
    rv = h1->remove(h, cookie, item);
    assert(rv == ENGINE_KEY_ENOENT);

    return SUCCESS;
}

static enum test_result test_two_engines_no_autocreate(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    item *item = NULL, *fetched_item;
    const void *cookie = mk_conn("autouser");
    char *key = "somekey";
    char *value = "some value";
    uint64_t cas_out = 0, result = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    rv = h1->store(h, cookie, item, 0, OPERATION_SET);
    assert(rv == ENGINE_NOT_STORED);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key));
    assert(rv == ENGINE_KEY_ENOENT);

    rv = h1->remove(h, cookie, item);
    assert(rv == ENGINE_KEY_ENOENT);

    rv = h1->arithmetic(h, cookie, key, strlen(key),
                        true, true, 1, 1, 0, &cas_out, &result);
    assert(rv == ENGINE_KEY_ENOENT);

    // no effect, but increases coverage.
    h1->reset_stats(h, cookie);

    return SUCCESS;
}

static enum test_result test_no_default_storage(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    item *item = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL);
    char *key = "somekey";
    char *value = "some value";

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key));
    assert(rv == ENGINE_KEY_ENOENT);


    return SUCCESS;
}

static enum test_result test_two_engines(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1"), *cookie2 = mk_conn("user2");
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);

    assert(!item_eq(h, h1, fetched_item1, fetched_item2));
    assert_item_eq(h, h1, item1, fetched_item1);
    assert_item_eq(h, h1, item2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_two_engines_del(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1"), *cookie2 = mk_conn("user2");
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Delete an item
    rv = h1->remove(h, cookie1, item1);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key));
    assert(rv == ENGINE_KEY_ENOENT);
    assert(fetched_item1 == NULL);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, item2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_two_engines_flush(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1"), *cookie2 = mk_conn("user2");
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // flush it
    rv = h1->flush(h, cookie1, 0);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key));
    assert(rv == ENGINE_KEY_ENOENT);
    assert(fetched_item1 == NULL);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, item2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_arith(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = mk_conn("user1"), *cookie2 = mk_conn("user2");
    char *key = "somekey";
    uint64_t result = 0, cas = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Initialize the first one.
    rv = h1->arithmetic(h, cookie1, key, strlen(key),
                        true, true, 1, 1, 0, &cas, &result);
    assert(rv == ENGINE_SUCCESS);
    assert(cas == 0);
    assert(result == 1);

    // Fail an init of the second one.
    rv = h1->arithmetic(h, cookie2, key, strlen(key),
                        true, false, 1, 1, 0, &cas, &result);
    assert(rv == ENGINE_KEY_ENOENT);

    // Update the first again.
    rv = h1->arithmetic(h, cookie1, key, strlen(key),
                        true, true, 1, 1, 0, &cas, &result);
    assert(rv == ENGINE_SUCCESS);
    assert(cas == 0);
    assert(result == 2);

    return SUCCESS;
}

static enum test_result test_get_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *info = h1->get_info(h);
    return strncmp(info, "Bucket engine", 13) == 0 ? SUCCESS : FAIL;
}

static void* create_packet(uint8_t opcode, const char *key, const char *val) {
    void *pkt_raw = calloc(1,
                           sizeof(protocol_binary_request_header)
                           + strlen(key)
                           + strlen(val));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.bodylen = htonl(strlen(key) + strlen(val));
    req->request.keylen = htons(strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header),
           key, strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header) + strlen(key),
           val, strlen(val));
    return pkt_raw;
}

static enum test_result test_create_bucket(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin");
    const char *key = "somekey";
    const char *value = "the value";
    item *item;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    rv = h1->allocate(h, mk_conn("someuser"), &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->allocate(h, mk_conn("someuser"), &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_double_create_bucket(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin");
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == ENGINE_KEY_EEXISTS);

    return SUCCESS;
}

static enum test_result test_create_bucket_with_params(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin"), *other_cookie = mk_conn("someuser");
    const char *key = "somekey";
    const char *value = "the value";
    item *item;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    rv = h1->allocate(h, adm_cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "no_alloc");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->allocate(h, other_cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    return SUCCESS;
}

static enum test_result test_admin_user(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Test with no user.
    void *pkt = create_packet(CREATE_BUCKET, "newbucket", "");
    rv = h1->unknown_command(h, mk_conn(NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    // Test with non-admin
    pkt = create_packet(CREATE_BUCKET, "newbucket", "");
    rv = h1->unknown_command(h, mk_conn("notadmin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    // Test with admin
    pkt = create_packet(CREATE_BUCKET, "newbucket", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    return SUCCESS;
}

static enum test_result test_delete_bucket(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin");
    const char *key = "somekey";
    const char *value = "the value";
    item *item;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    const void *other_cookie = mk_conn("someuser");

    rv = h1->allocate(h, other_cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    pkt = create_packet(DELETE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);

    pkt = create_packet(DELETE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == ENGINE_KEY_ENOENT);

    rv = h1->allocate(h, other_cookie, &item,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    return SUCCESS;
}

static enum test_result test_bucket_name_validation(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "bucket one", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_NOT_STORED);

    return SUCCESS;
}

static enum test_result test_list_buckets_none(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Go find all the buckets.
    void *pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    // Now verify the body looks alright.
    assert(last_body == NULL);

    return SUCCESS;
}

static enum test_result test_list_buckets_one(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Create a bucket first.

    void *pkt = create_packet(CREATE_BUCKET, "bucket1", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    // Now go find all the buckets.
    pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    // Now verify the body looks alright.
    assert(strncmp(last_body, "bucket1", 7) == 0);

    return SUCCESS;
}

static enum test_result test_list_buckets_two(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const void *cookie = mk_conn("admin");
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    // Create two buckets first.

    void *pkt = create_packet(CREATE_BUCKET, "bucket1", "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(CREATE_BUCKET, "bucket2", "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    // Now go find all the buckets.
    pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    // Now verify the body looks alright.
    assert(strncmp(last_body, "bucket1 bucket2", 15) == 0
           || strncmp(last_body, "bucket2 bucket1", 15) == 0);

    return SUCCESS;
}

static enum test_result test_expand_bucket(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "bucket1", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(EXPAND_BUCKET, "bucket1", "1024");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    return SUCCESS;
}

static enum test_result test_expand_missing_bucket(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(EXPAND_BUCKET, "bucket1", "1024");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    const char *exp = "Engine not found";
    assert(memcmp(last_key, exp, strlen(exp)) == 0);

    return SUCCESS;
}

static enum test_result test_unknown_call(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(0xfe, "somekey", "someval");
    rv = h1->unknown_command(h, mk_conn("someuser"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    return SUCCESS;
}

static enum test_result test_select_no_admin(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(CREATE_BUCKET, "someuser", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(SELECT_BUCKET, "stuff", "");
    rv = h1->unknown_command(h, mk_conn("notadmin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    return SUCCESS;
}

static enum test_result test_select_no_bucket(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(SELECT_BUCKET, "stuff", "");
    rv = h1->unknown_command(h, mk_conn("admin"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == ENGINE_KEY_ENOENT);

    return SUCCESS;
}

static enum test_result test_select(ENGINE_HANDLE *h,
                                    ENGINE_HANDLE_V1 *h1) {
    item *item1, *fetched_item1 = NULL, *fetched_item2;
    const void *cookie1 = mk_conn("user1"), *admin = mk_conn("admin");
    char *key = "somekey";
    char *value1 = "some value1";

    store(h, h1, cookie1, key, value1, &item1);

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);
    rv = h1->get(h, admin, &fetched_item2, key, strlen(key));
    assert(rv == ENGINE_KEY_ENOENT);

    assert_item_eq(h, h1, item1, fetched_item1);

    void *pkt = create_packet(SELECT_BUCKET, "user1", "");
    rv = h1->unknown_command(h, admin, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->get(h, admin, &fetched_item2, key, strlen(key));
    assert(rv == ENGINE_SUCCESS);
    assert_item_eq(h, h1, item1, fetched_item2);

    return SUCCESS;
}

static enum test_result test_unknown_call_no_bucket(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(0xfe, "somekey", "someval");
    rv = h1->unknown_command(h, mk_conn("someuser"), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    return SUCCESS;
}

static ENGINE_HANDLE_V1 *start_your_engines(const char *cfg) {
    ENGINE_HANDLE_V1 *h = (ENGINE_HANDLE_V1 *)load_engine(".libs/bucket_engine.so",
                                                          cfg);
    assert(h);
    // printf("Engine:  %s\n", h->get_info((ENGINE_HANDLE*)h));
    return h;
}

static int report_test(enum test_result r) {
    int rc = 0;
    char *msg = NULL;
    bool color_enabled = getenv("TESTAPP_ENABLE_COLOR") != NULL;
    int color = 0;
    char color_str[8] = { 0 };
    char *reset_color = "\033[m";
    switch(r) {
    case SUCCESS:
        msg="OK";
        color = 32;
        break;
    case FAIL:
        color = 31;
        msg="FAIL";
        rc = 1;
        break;
    case DIED:
        color = 31;
        msg = "DIED";
        rc = 1;
        break;
    case CORE:
        color = 31;
        msg = "CORE DUMPED";
        rc = 1;
        break;
    case PENDING:
        color = 33;
        msg = "PENDING";
        break;
    }
    assert(msg);
    if (color_enabled) {
        snprintf(color_str, sizeof(color_str), "\033[%dm", color);
    }
    printf("%s%s%s\n", color_str, msg, color_enabled ? reset_color : "");
    return rc;
}

static void disconnect_all_connections(struct connstruct *c) {
    if (c) {
        disconnect(c);
        disconnect_all_connections(c->next);
        free((void*)c->uname);
        free(c);
    }
}

static void destroy_event_handlers_rec(struct engine_event_handler *h) {
    if (h) {
        destroy_event_handlers_rec(h->next);
        free(h);
    }
}

static void destroy_event_handlers() {
    int i = 0;
    for (i = 0; i < MAX_ENGINE_EVENT_TYPE; i++) {
        destroy_event_handlers_rec(engine_event_handlers[i]);
        engine_event_handlers[i] = NULL;
    }
}

static enum test_result run_test(struct test test) {
    enum test_result ret = PENDING;
    if (test.tfun != NULL) {
        last_status = 0xff;
#ifndef USE_GCOV
        pid_t pid = fork();
        if (pid == 0) {
#endif
            /* Start the engines and go */
            ENGINE_HANDLE_V1 *h = start_your_engines(test.cfg ?: DEFAULT_CONFIG);
            ret = test.tfun((ENGINE_HANDLE*)h, h);
            disconnect_all_connections(connstructs);
            destroy_event_handlers();
            connstructs = NULL;
            h->destroy((ENGINE_HANDLE*)h);
#ifndef USE_GCOV
            exit((int)ret);
        } else if (pid == (pid_t)-1) {
            ret = FAIL;
        } else {
            int rc;
            while (waitpid(pid, &rc, 0) == (pid_t)-1) {
                if (errno != EINTR) {
                    abort();
                }
            }

            if (WIFEXITED(rc)) {
                ret = (enum test_result)WEXITSTATUS(rc);
            } else if (WIFSIGNALED(rc) && WCOREDUMP(rc)) {
                ret = CORE;
            } else {
                ret = DIED;
            }
        }
#endif
    }

    return ret;
}

int main(int argc, char **argv) {
    int i = 0;
    int rc = 0;

    struct test tests[] = {
        {"get info", test_get_info},
        {"default storage", test_default_storage},
        {"default storage key overrun", test_default_storage_key_overrun},
        {"default unlinked remove", test_default_unlinked_remove},
        {"no default storage",
         test_no_default_storage,
         "engine=.libs/mock_engine.so;default=false"},
        {"user storage with no default",
         test_two_engines,
         "engine=.libs/mock_engine.so;default=false"},
        {"distinct storage", test_two_engines, DEFAULT_CONFIG_AC},
        {"distinct storage (no auto-create)", test_two_engines_no_autocreate,
         DEFAULT_CONFIG_NO_DEF},
        {"delete from one of two nodes", test_two_engines_del,
         DEFAULT_CONFIG_AC},
        {"flush from one of two nodes", test_two_engines_flush,
         DEFAULT_CONFIG_AC},
        {"isolated arithmetic", test_arith, DEFAULT_CONFIG_AC},
        {"create bucket", test_create_bucket, DEFAULT_CONFIG_NO_DEF},
        {"double create bucket", test_double_create_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"create bucket with params", test_create_bucket_with_params,
         DEFAULT_CONFIG_NO_DEF},
        {"bucket name verification", test_bucket_name_validation},
        {"delete bucket", test_delete_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"expand bucket", test_expand_bucket},
        {"expand missing bucket", test_expand_missing_bucket},
        {"list buckets with none", test_list_buckets_none},
        {"list buckets with one", test_list_buckets_one},
        {"list buckets", test_list_buckets_two},
        {"fail to select a bucket when not admin", test_select_no_admin},
        {"select a bucket as admin", test_select, DEFAULT_CONFIG_AC},
        {"fail to select non-existent bucket as admin", test_select_no_bucket},
        {"stats call"},
        {"release call"},
        {"unknown call delegation", test_unknown_call},
        {"unknown call delegation (no bucket)", test_unknown_call_no_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"admin verification", test_admin_user},
        {NULL, NULL}
    };

    for (i = 0; tests[i].name; i++) {
        printf("Running %s... ", tests[i].name);
        fflush(stdout);
	rc += report_test(run_test(tests[i]));
    }

    return rc;
}
