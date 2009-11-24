common_CFLAGS=-g -O0
common_LDFLAGS=-g

lib_LTLIBRARIES = bucket_engine.la
bucket_engine_la_SOURCES= bucket_engine.c config_parser.c config_parser.h
bucket_la_CFLAGS = $(common_CFLAGS)
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

test: testapp
	./testapp
