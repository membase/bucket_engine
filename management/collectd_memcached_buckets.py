#!/usr/bin/python
import sys, time, re
sys.path = ['/usr/local/lib/python2.6/site-packages'] + sys.path

import mc_bin_client
import collectd

"""
memcached_command       value:COUNTER:0:U
memcached_connections   value:GAUGE:0:U
memcached_items         value:GAUGE:0:U
memcached_octets        rx:COUNTER:0:4294967295, tx:COUNTER:0:4294967295
memcached_ops           value:COUNTER:0:134217728

APPEND_STAT("cmd_get", "%"PRIu64, thread_stats.get_cmds);
APPEND_STAT("cmd_set", "%"PRIu64, slab_stats.set_cmds);
APPEND_STAT("cmd_flush", "%"PRIu64, thread_stats.flush_cmds);
APPEND_STAT("get_hits", "%"PRIu64, slab_stats.get_hits);
APPEND_STAT("get_misses", "%"PRIu64, thread_stats.get_misses);
APPEND_STAT("delete_misses", "%"PRIu64, thread_stats.delete_misses);
APPEND_STAT("delete_hits", "%"PRIu64, slab_stats.delete_hits);
APPEND_STAT("incr_misses", "%"PRIu64, thread_stats.incr_misses);
APPEND_STAT("incr_hits", "%"PRIu64, thread_stats.incr_hits);
APPEND_STAT("decr_misses", "%"PRIu64, thread_stats.decr_misses);
APPEND_STAT("decr_hits", "%"PRIu64, thread_stats.decr_hits);
APPEND_STAT("cas_misses", "%"PRIu64, thread_stats.cas_misses);
APPEND_STAT("cas_hits", "%"PRIu64, slab_stats.cas_hits);
APPEND_STAT("cas_badval", "%"PRIu64, slab_stats.cas_badval);
APPEND_STAT("bytes_read", "%"PRIu64, thread_stats.bytes_read);
APPEND_STAT("bytes_written", "%"PRIu64, thread_stats.bytes_written);


"""

MASK = 134217727

def get_hostname_from_collectd_config(fn):
    data = open(fn).read()
    r = re.compile('^Hostname\s*"([^"]+)"$', re.IGNORECASE|re.MULTILINE)
    m = r.search(data)
    return m.group(1) if m is not None else None


def get_bucket_stats(mc):
    ops = ['cmd_get',
           'cmd_set',
           'cmd_flush',
           'get_hits',
           'get_misses',
           'delete_misses',
           'delete_hits',
           'incr_misses',
           'incr_hits',
           'decr_misses',
           'decr_hits',
           'cas_misses',
           'cas_hits',
           'cas_badval',
           'bytes_read',
           'bytes_written',
           'evictions',
          ]

    stats = mc.stats()
    return [ int(stats[op]) & MASK for op in ops ]


def put_values(c, hostname, bucket, now, values):
    identifier = collectd.Identifier(hostname, 'memcached_bucket', None, 'memcached_bucket', bucket)
    c.putval(identifier, now, values)


def list_buckets(mc):
    # return mc.bucket_list()
    buckets = []
    for line in open('/var/db/memcached.pw'):
        buckets.append(line.split()[0])

    return buckets

def main():
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('-H', '--hostname', dest='hostname')
    parser.add_option('-m', '--memcached-host', dest='memcached_host')
    parser.add_option('-u', '--memcached-username', dest='memcached_username')
    parser.add_option('-p', '--memcached-password', dest='memcached_password')

    options, args = parser.parse_args()

    mc = mc_bin_client.MemcachedClient(options.memcached_host or '127.0.0.1')
    if options.memcached_username:
        mc.sasl_auth_plain(options.memcached_username, options.memcached_password)

    hostname = options.hostname or get_hostname_from_collectd_config('/etc/collectd/collectd.conf') 
    assert hostname
    c = collectd.Exec()
    l = list_buckets(mc)
    now = time.time()
    totals = None
    for bucket in l:
        try:
            mc.bucket_select(bucket)
            values = get_bucket_stats(mc)
            if totals is None:
                totals = values[:]
            else:
                for i in xrange(len(totals)):
                    totals[i] += values[i]
                    totals[i] &= MASK

            put_values(c, hostname, bucket, now, values)
        except:
            #import traceback
            #traceback.print_exc()
            pass

    put_values(c, hostname, 'ALL', now, totals)


if __name__ == '__main__':
    main()

