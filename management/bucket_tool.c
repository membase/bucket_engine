/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/protocol_binary.h>

#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "bucket_engine.h"

const char *e2t(protocol_binary_response_status error) {
    static char buffer[100];

    switch (error) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS: return "success";
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT: return "not found";
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS: return "already exists";
    case PROTOCOL_BINARY_RESPONSE_E2BIG: return "too big";
    case PROTOCOL_BINARY_RESPONSE_EINVAL: return "invalid arguments";
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED: return "not stored";
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL: return "bad value";
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET: return "not my vbucket";
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR: return "authentication error";
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE: return "auth cont";
    case PROTOCOL_BINARY_RESPONSE_ERANGE: return "invalid range";
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND: return "unknown command";
    case PROTOCOL_BINARY_RESPONSE_ENOMEM: return "out of memory";
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED: return "not supported";
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL: return "internal error";
    case PROTOCOL_BINARY_RESPONSE_EBUSY: return "too busy";
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL: return "temporary failure.. try agail";
    }
    snprintf(buffer, sizeof(buffer), "unknown (%0x)", error);
    return buffer;
}

static void usage(void)
{
    fprintf(stderr, "Usage bucket_adm [-h host[:port]] [-p port] <cmd>\n");
    fprintf(stderr, "  where <cmd> may be one of:\n");
    fprintf(stderr, "\tlist - List all configured buckets\n");
    fprintf(stderr, "\tdelete <name> [force=true] - Delete named bucket\n");
    fprintf(stderr, "\tcreate <name> <module> [config] - Create named bucket\n");
    exit(EXIT_FAILURE);
}

/**
 * Send the chunk of data to the other side, retry if an error occurs
 * (or terminate the program if retry wouldn't help us)
 * @param sock socket to write data to
 * @param buf buffer to send
 * @param len length of data to send
 */
static void retry_send(int sock, const void* buf, size_t len)
{
    off_t offset = 0;
    const char* ptr = buf;

    do {
        size_t num_bytes = len - offset;
        ssize_t nw = send(sock, ptr + offset, num_bytes, 0);
        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to write: %s\n", strerror(errno));
                close(sock);
                exit(1);
            }
        } else {
            offset += nw;
        }
    } while (offset < len);
}

/**
 * Receive a fixed number of bytes from the socket.
 * (Terminate the program if we encounter a hard error...)
 * @param sock socket to receive data from
 * @param buf buffer to store data to
 * @param len length of data to receive
 */
static void retry_recv(int sock, void *buf, size_t len)
{
    if (len == 0) {
        return;
    }
    off_t offset = 0;
    do {
        ssize_t nr = recv(sock, ((char*)buf) + offset, len - offset, 0);
        if (nr == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to read: %s\n", strerror(errno));
                close(sock);
                exit(1);
            }
        } else {
            if (nr == 0) {
                fprintf(stderr, "Connection closed\n");
                close(sock);
                exit(1);
            }
            offset += nr;
        }
    } while (offset < len);
}

static int do_sasl_auth(int sock, const char *user, const char *pass)
{
    /*
     * For now just shortcut the SASL phase by requesting a "PLAIN"
     * sasl authentication.
     */
    size_t ulen = strlen(user) + 1;
    size_t plen = pass ? strlen(pass) + 1 : 1;
    size_t tlen = ulen + plen + 1;

    protocol_binary_request_stats request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = PROTOCOL_BINARY_CMD_SASL_AUTH,
            .keylen = htons(5),
            .bodylen = htonl(5 + tlen)
        }
    };

    retry_send(sock, &request, sizeof(request));
    retry_send(sock, "PLAIN", 5);
    retry_send(sock, "", 1);
    retry_send(sock, user, ulen);
    if (pass) {
        retry_send(sock, pass, plen);
    } else {
        retry_send(sock, "", 1);
    }

    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    uint32_t vallen = ntohl(response.message.header.response.bodylen);
    char *buffer = NULL;

    if (vallen != 0) {
        buffer = malloc(vallen);
        retry_recv(sock, buffer, vallen);
    }

    protocol_binary_response_status status;
    status = ntohs(response.message.header.response.status);

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Failed to authenticate to the server: %s\n",
                e2t(status));
        close(sock);
        sock = -1;
        return -1;
    }

    free(buffer);
    return sock;
}

/**
 * Try to connect to the server
 * @param host the name of the server
 * @param port the port to connect to
 * @param user the username to use for SASL auth (NULL = NO SASL)
 * @param pass the password to use for SASL auth
 * @return a socket descriptor connected to host:port for success, -1 otherwise
 */
static int connect_server(const char *hostname, const char *port,
                          const char *user, const char *pass)
{
    struct addrinfo *ainfo = NULL;
    struct addrinfo hints = {
        .ai_flags = AI_ALL,
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP};

    if (getaddrinfo(hostname, port, &hints, &ainfo) != 0) {
        return -1;
    }

    int sock = -1;
    struct addrinfo *ai = ainfo;
    while (ai != NULL) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock != -1) {
            if (connect(sock, ai->ai_addr, ai->ai_addrlen) != -1) {
                break;
            }
            close(sock);
            sock = -1;
        }
        ai = ai->ai_next;
    }

    freeaddrinfo(ainfo);

    if (sock == -1) {
        fprintf(stderr, "Failed to connect to memcached server (%s:%s): %s\n",
                hostname, port, strerror(errno));
    } else if (user != NULL && do_sasl_auth(sock, user, pass) == -1) {
        close(sock);
        sock = -1;
    }

    return sock;
}

static void dump_extra_info(int sock, uint32_t nb)
{
    if (nb > 0) {
        char *payload = calloc(1, nb + 1);
        char *curr = payload;
        bool done = false;
        retry_recv(sock, payload, nb);
        fprintf(stderr, "\t%s\n", payload);
        free(payload);
    }
}

static int create(int sock, char **argv, int offset, int argc)
{
    char *name;
    uint16_t namelen;
    char *engine;
    uint16_t nengine;
    char *config = NULL;
    uint32_t nconfig = 0;

    if (offset == argc) {
        fprintf(stderr, "You have to specify the bucket to create\n");
        return EXIT_FAILURE;
    }

    name = argv[offset++];
    namelen = (uint16_t)strlen(name);

    if (offset == argc) {
        fprintf(stderr, "You have to specify the engine for the bucket\n");
        return EXIT_FAILURE;
    }
    engine = argv[offset++];
    nengine = (uint16_t)strlen(engine) + 1; // Include '\0'

    if (offset < argc) {
        config = argv[offset++];
        nconfig = (uint32_t)strlen(config);
    }
    if (offset != argc) {
        fprintf(stderr, "Too many arguments to create\n");
        return EXIT_FAILURE;
    }

    protocol_binary_request_create_bucket request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = CREATE_BUCKET,
            .keylen = htons(namelen),
            .bodylen = htonl(nconfig + namelen + nengine)
        }
    };

    retry_send(sock, &request, sizeof(request));
    retry_send(sock, name, namelen);
    retry_send(sock, engine, nengine);
    if (config) {
        retry_send(sock, config, nconfig);
    }

    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    uint32_t nb = ntohl(response.message.header.response.bodylen);
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Failed to create bucket \"%s\": %s\n", name,
                e2t(err));
        dump_extra_info(sock, nb);
        return EXIT_FAILURE;
    } else {
        fprintf(stdout, "Bucket \"%s\" successfully created\n", name);
    }

    return EXIT_SUCCESS;
}

static int delete(int sock, char **argv, int offset, int argc)
{
    char *name;
    uint16_t namelen;
    char *config = NULL;
    uint32_t nconfig = 0;

    if (offset == argc) {
        fprintf(stderr, "You have to specify the bucket to delete\n");
        return EXIT_FAILURE;
    }

    name = argv[offset++];
    namelen = (uint16_t)strlen(name);

    if (offset < argc) {
        config = argv[offset++];
        nconfig = (uint32_t)strlen(config);
    }
    if (offset != argc) {
        fprintf(stderr, "Too many arguments to delete\n");
        return EXIT_FAILURE;
    }

    protocol_binary_request_delete_bucket request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = DELETE_BUCKET,
            .keylen = htons(namelen),
            .bodylen = htonl(nconfig + namelen)
        }
    };

    retry_send(sock, &request, sizeof(request));
    retry_send(sock, name, namelen);
    if (config) {
        retry_send(sock, config, nconfig);
    }

    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    uint32_t nb = ntohl(response.message.header.response.bodylen);
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Failed to delete bucket \"%s\": %s\n", name,
                e2t(err));
        dump_extra_info(sock, nb);
        return EXIT_FAILURE;
    } else {
        fprintf(stdout, "Bucket \"%s\" successfully deleted\n", name);
    }

    return EXIT_SUCCESS;
}

static int list(int sock, char **argv, int offset, int argc)
{
    if (offset != argc) {
        fprintf(stderr, "ERROR - Unknown arguments to list\n");
        return EXIT_FAILURE;
    }

    /* list buckets does not take any parameters */
    protocol_binary_request_no_extras request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = LIST_BUCKETS
        }
    };

    retry_send(sock, &request, sizeof(request));

    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    uint32_t nb = ntohl(response.message.header.response.bodylen);
    if (response.message.header.response.status != 0) {
        uint16_t err = ntohs(response.message.header.response.status);
        fprintf(stderr, "Failed to list bucket: %s\n", e2t(err));
        dump_extra_info(sock, nb);
        return EXIT_FAILURE;
    }

    if (nb == 0) {
        fprintf(stdout, "No buckets defined");
    } else {
        char *payload = calloc(1, nb + 1);
        char *curr = payload;
        bool done = false;
        retry_recv(sock, payload, nb);

        do {
            char *end = strchr(curr, ' ');
            if (end) {
                *end = '\0';
                end++;
            } else {
                done = true;
            }
            fprintf(stdout, "%s\n", curr);
            curr = end;
        } while (!done);
        free(payload);
    }

    return EXIT_SUCCESS;
}

/**
 * Program entry point.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv)
{
    int cmd;
    const char * const default_ports[] = { "memcache", "11211", NULL };
    const char *port = NULL;
    const char *host = NULL;
    const char *user = NULL;
    const char *pass = NULL;
    char *ptr;
    int (*command)(int sock, char **argv, int offset, int argc);
    int exitcode;

    while ((cmd = getopt(argc, argv, "h:p:u:P:")) != EOF) {
        switch (cmd) {
        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        case 'u' :
            user = optarg;
            break;
        case 'P':
            pass = optarg;
            break;
        default:
            usage();
        }
    }

    if (host == NULL) {
        host = "localhost";
    }

    if (optind < argc) {
        if (strcmp(argv[optind], "create") == 0) {
            command = create;
        } else if (strcmp(argv[optind], "delete") == 0) {
            command = delete;
        } else if (strcmp(argv[optind], "list") == 0) {
            command = list;
        } else {
            usage();
            /* NOTREACHED */
        }
    } else {
        usage();
        /* NOTREACHED */
    }

    int sock = -1;
    if (port == NULL) {
        int ii = 0;
        do {
            port = default_ports[ii++];
            sock = connect_server(host, port, user, pass);
        } while (sock == -1 && default_ports[ii] != NULL);
    } else {
        sock = connect_server(host, port, user, pass);
    }

    if (sock == -1) {
        return 1;
    }

    exitcode = command(sock, argv, ++optind, argc);
    close(sock);
    exit(exitcode);
}
