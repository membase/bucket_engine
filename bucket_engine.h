#ifndef BUCKET_ENGINE_H
#define BUCKET_ENGINE_H 1

#include <memcached/protocol_binary.h>

#define CREATE_BUCKET 0x25
#define DELETE_BUCKET 0x26
#define LIST_BUCKETS  0x27

typedef protocol_binary_request_no_extras protocol_binary_request_create_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_delete_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_list_buckets;

#endif /* BUCKET_ENGINE_H */
