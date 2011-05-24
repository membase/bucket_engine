#ifndef BUCKET_ENGINE_H
#define BUCKET_ENGINE_H 1

#include <memcached/protocol_binary.h>

/* Membase up to 1.7 use command ids from the reserved
 * set. Let's deprecate them (but still accept them)
 * and drop support for them when we move to 2.0
 */
#define CREATE_BUCKET_DEPRECATED 0x25
#define DELETE_BUCKET_DEPRECATED 0x26
#define LIST_BUCKETS_DEPRECATED  0x27
#define SELECT_BUCKET_DEPRECATED 0x29

#define CREATE_BUCKET 0x85
#define DELETE_BUCKET 0x86
#define LIST_BUCKETS  0x87
#define SELECT_BUCKET 0x89

typedef protocol_binary_request_no_extras protocol_binary_request_create_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_delete_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_list_buckets;
typedef protocol_binary_request_no_extras protocol_binary_request_select_bucket;

#endif /* BUCKET_ENGINE_H */
