ACLOCAL_AMFLAGS = -I m4 --force

memcachedlibdir = $(libdir)/memcached
memcachedlib_LTLIBRARIES = bucket_engine.la
bucket_engine_la_SOURCES= bucket_engine.c bucket_engine.h \
                          topkeys.c topkeys.h bucket_engine_internal.h

bucket_engine_la_LDFLAGS= -module -dynamic -R '$(memcachedlibdir)' \
                          -avoid-version
bucket_engine_la_LIBADD = libgenhash.la

noinst_LTLIBRARIES = mock_engine.la libgenhash.la
libgenhash_la_SOURCES = genhash.c genhash.h genhash_int.h
mock_engine_la_SOURCES = mock_engine.c
mock_engine_la_LDFLAGS = -module -dynamic -rpath /nowhere
mock_engine_la_LIBADD = libgenhash.la

bin_PROGRAMS =
noinst_PROGRAMS = testapp
testapp_SOURCES = testapp.c
testapp_DEPENDENCIES = mock_engine.la libgenhash.la bucket_engine.la
testapp_LDADD = libgenhash.la

if BUILD_MANAGEMENT
bin_PROGRAMS += management/bucket_tool
management_bucket_tool_SOURCES = management/bucket_tool.c
endif

EXTRA_DIST = management win32 README.markdown bucket_engine.spec

check_PROGRAMS = testapp

TESTS=${check_PROGRAMS}

test: check
