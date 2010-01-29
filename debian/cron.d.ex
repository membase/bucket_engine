#
# Regular cron jobs for the memcached-bucket-engine package
#
0 4	* * *	root	[ -x /usr/bin/memcached-bucket-engine_maintenance ] && /usr/bin/memcached-bucket-engine_maintenance
