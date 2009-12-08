#!/usr/bin/env python

import sys

import mc_bin_client

if __name__ == '__main__':
    mc = mc_bin_client.MemcachedClient(sys.argv[1])
    mc.sasl_auth_cram_md5(sys.argv[2], sys.argv[3])
    for b in sorted(mc.bucket_list()):
        print b

