#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <assert.h>

#include "memcached/engine.h"

struct test {
    const char *name;
    void (*tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
};

static const char* get_server_version() {
    return "bucket mock";
}

static const char* get_auth_data(const void *cookie) {
    return (const char*)cookie;
}

static void register_callback(ENGINE_EVENT_TYPE type, EVENT_CALLBACK cb) {
    // Nothing yet.
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
        .server_version = get_server_version
    };

    if (interface != 1) {
        return NULL;
    }

    return &server_api;
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

static void test_default_storage(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *item = NULL, *fetched_item;
    const void *cookie = NULL;
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
}

static void test_two_engines(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = "user1", *cookie2 = "user2";
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
}

static void test_get_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *info = h1->get_info(h);
    assert(strncmp(info, "Bucket engine", 13) == 0);
}

static ENGINE_HANDLE_V1 *start_your_engines() {
    const char *cfg = "engine=.libs/mock_engine.so";
    ENGINE_HANDLE_V1 *h = (ENGINE_HANDLE_V1 *)load_engine(".libs/bucket_engine.so",
                                                          cfg);
    assert(h);
    // printf("Engine:  %s\n", h->get_info((ENGINE_HANDLE*)h));
    return h;
}

int main(int argc, char **argv) {
    int i = 0;

    struct test tests[] = {
        {"get info", test_get_info},
        {"default storage", test_default_storage},
        {"distinct storage", test_two_engines},
        {NULL, NULL}
    };

    for (i = 0; tests[i].name; i++) {
        printf("Running %s... ", tests[i].name);
        fflush(stdout);
        ENGINE_HANDLE_V1 *h = start_your_engines();
        tests[i].tfun((ENGINE_HANDLE*)h, h);
        h->destroy((ENGINE_HANDLE*)h);
        printf("OK\n");
    }

    return 0;
}
