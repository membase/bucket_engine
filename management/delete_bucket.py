#!/usr/bin/env python

import sys

import mc_bin_client

if __name__ == '__main__':
    mc = mc_bin_client.MemcachedClient(sys.argv[1])
    mc.sasl_auth_plain(sys.argv[2], sys.argv[3])
    config = "force=false"
    if len(sys.argv) == 6:
        config = sys.argv[5]
    mc.bucket_delete(sys.argv[4], config)
