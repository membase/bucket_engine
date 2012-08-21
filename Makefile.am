ACLOCAL_AMFLAGS = -I m4 --force

memcachedlibdir = $(libdir)/memcached
memcachedlib_LTLIBRARIES = bucket_engine.la
bucket_engine_la_SOURCES= bucket_engine.c bucket_engine.h \
                          topkeys.c topkeys.h bucket_engine_internal.h

bucket_engine_la_LDFLAGS= -module -dynamic -R '$(memcachedlibdir)'
bucket_engine_la_LIBADD = libgenhash.la

noinst_LTLIBRARIES = mock_engine.la libgenhash.la
libgenhash_la_SOURCES = genhash.c genhash.h genhash_int.h
mock_engine_la_SOURCES = mock_engine.c
mock_engine_la_LDFLAGS = -module -dynamic -rpath /nowhere
mock_engine_la_LIBADD = libgenhash.la

noinst_PROGRAMS = testapp
testapp_SOURCES = testapp.c
testapp_DEPENDENCIES = mock_engine.la libgenhash.la bucket_engine.la
testapp_LDADD = libgenhash.la

pythonlibdir=$(libdir)/python
pythonlib_DATA= \
                management/mc_bin_client.py \
                management/memcacheConstants.py \
                management/types.db

pythonlib_SCRIPTS= \
                management/collectd.py \
                management/collectd_memcached_buckets.py

PYTHON_TOOLS= \
                management/collectd \
                management/collectd_memcached_buckets

${PYTHON_TOOLS}: python_wrapper
	cp $< $@

BUILT_SOURCES = ${PYTHON_TOOLS}
CLEANFILES = ${PYTHON_TOOLS}
bin_SCRIPTS = ${PYTHON_TOOLS}

pkgdata_SCRIPTS = management/mc_bin_client.py                   \
                  management/memcacheConstants.py               \
                  management/collectd.py                        \
                  management/collectd_memcached_buckets.py      \
                  management/types.db

EXTRA_DIST = $(pkgdata_SCRIPTS) win32 README.markdown bucket_engine.spec

check_PROGRAMS = testapp

TESTS=${check_PROGRAMS}

test: check
