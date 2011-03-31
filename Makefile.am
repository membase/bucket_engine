TMP_DIR=./tmp

TMP_VER=$(TMP_DIR)/version_num.tmp

memcachedlibdir = $(libdir)/memcached
memcachedlib_LTLIBRARIES = bucket_engine.la
bucket_engine_la_SOURCES= bucket_engine.c bucket_engine.h
bucket_engine_la_LDFLAGS= -module -dynamic
bucket_engine_la_LIBADD = libgenhash.la

noinst_LTLIBRARIES = mock_engine.la libgenhash.la
libgenhash_la_SOURCES = genhash.c genhash.h genhash_int.h
mock_engine_la_SOURCES = mock_engine.c
mock_engine_la_LDFLAGS = -module -dynamic -rpath /nowhere
mock_engine_la_LIBADD = libgenhash.la

noinst_PROGRAMS = testapp
testapp_SOURCES = testapp.c
testapp_DEPENDENCIES = mock_engine.la libgenhash.la
testapp_LDADD = libgenhash.la

pythonlibdir=$(libdir)/python
pythonlib_DATA= \
                management/mc_bin_client.py \
                management/memcacheConstants.py \
                management/types.db

pythonlib_SCRIPTS= \
                management/collectd.py \
                management/collectd_memcached_buckets.py \
                management/create_bucket.py \
                management/delete_bucket.py \
                management/list_buckets.py

PYTHON_TOOLS= \
                management/collectd \
                management/collectd_memcached_buckets \
                management/create_bucket \
                management/delete_bucket \
                management/list_buckets

${PYTHON_TOOLS}: management/python_wrapper
	cp $< $@

BUILT_SOURCES = ${PYTHON_TOOLS}
CLEANFILES = ${PYTHON_TOOLS}
bin_SCRIPTS = ${PYTHON_TOOLS}

pkgdata_SCRIPTS = management/create_bucket.py  management/delete_bucket.py  management/list_buckets.py  management/mc_bin_client.py  management/memcacheConstants.py management/collectd.py management/collectd_memcached_buckets.py management/types.db

EXTRA_DIST = $(pkgdata_SCRIPTS) win32 README.markdown

test: all testapp
	./testapp

version:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)

bdist: version
	rm -f ./bucket_engine_*.tar.gz
	rm -rf $(TMP_DIR)/bucket_engine
	mkdir $(TMP_DIR)/bucket_engine
	cp .libs/bucket_engine.so $(TMP_DIR)/bucket_engine
	(cd $(TMP_DIR); tar cf - bucket_engine) | gzip -9 > bucket_engine_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created bucket_engine_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
