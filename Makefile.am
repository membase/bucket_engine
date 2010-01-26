TMP_DIR=./tmp

TMP_VER=$(TMP_DIR)/version_num.tmp

common_CFLAGS=@PROFILER_FLAGS@
common_LDFLAGS=-g @PROFILER_LDFLAGS@

lib_LTLIBRARIES = bucket_engine.la
bucket_engine_la_SOURCES= bucket_engine.c \
						  genhash.h genhash.c genhash_int.h bucket_engine.h
bucket_engine_la_CFLAGS = $(common_CFLAGS)
bucket_engine_la_LDFLAGS= -module -dynamic $(common_LDFLAGS)

noinst_LTLIBRARIES = mock_engine.la
mock_engine_la_SOURCES = mock_engine.c genhash.c genhash.h
mock_engine_la_CFLAGS = $(common_CFLAGS)
mock_engine_la_LDFLAGS = -module -dynamic -rpath /nowhere $(common_LDFLAGS)

noinst_PROGRAMS = testapp
testapp_SOURCES = testapp.c
testapp_DEPENDENCIES = mock_engine.la 
testapp_CFLAGS = $(common_CFLAGS)
testapp_LDFLAGS = $(common_LDFLAGS)

pkgdata_SCRIPTS = management/create_bucket.py  management/delete_bucket.py  management/list_buckets.py  management/mc_bin_client.py  management/memcacheConstants.py

test: testapp
	./testapp

version:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)

bdist: version
	rm -f ./bucket_engine_*.tar.gz
	rm -rf $(TMP_DIR)/bucket_engine
	mkdir $(TMP_DIR)/bucket_engine
	cp .libs/bucket_engine.so $(TMP_DIR)/bucket_engine
	tar --directory $(TMP_DIR) -czf bucket_engine_`cat $(TMP_VER)`.tar.gz bucket_engine
	echo created bucket_engine_`cat $(TMP_VER)`.tar.gz

