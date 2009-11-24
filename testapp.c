#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <assert.h>

#include "memcached/engine.h"

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

void test_storage(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
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

    assert(memcmp(h1->item_get_data(fetched_item), value, strlen(value)) == 0);
}

struct test {
    const char *name;
    void (*tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
};

int main(int argc, char **argv) {
    int i = 0;
    printf("Starting...\n");
    const char *cfg = "engine=.libs/mock_engine.so";
    ENGINE_HANDLE_V1 *h = (ENGINE_HANDLE_V1 *)load_engine(".libs/bucket_engine.so",
                                                          cfg);
    assert(h);
    printf("Engine:  %s\n", h->get_info((ENGINE_HANDLE*)h));

    struct test tests[] = {
        {"test_storage", test_storage},
        {NULL, NULL}
    };


    for (i = 0; tests[i].name; i++) {
        printf("Running %s... ", tests[i].name);
        fflush(stdout);
        tests[i].tfun((ENGINE_HANDLE*)h, h);
        printf("OK\n");
    }

    return 0;
}
