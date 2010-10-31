AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(bucket_engine, VERSION_NUMBER, dustin@spy.net)
AC_CONFIG_SRCDIR([bucket_engine.c])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_DISABLE_STATIC

PANDORA_CANONICAL_TARGET(less-warnings, warnings-always-on, force-gcc42)
PANDORA_REQUIRE_PTHREAD
PANDORA_ENABLE_VISIBILITY

AC_ARG_WITH([memcached],
    [AS_HELP_STRING([--with-memcached],
      [Memcached source location])],
    [ac_cv_with_memcached="$withval"],
    [ac_cv_with_memcached=""])

AS_IF(test "x${ac_cv_with_memcached}" != "x",
      [CPPFLAGS="-I${ac_cv_with_memcached}/include $CPPFLAGS"
       PATH="${ac_cv_with_memcached}:$PATH"])

AC_CHECK_HEADERS_ONCE(memcached/engine.h)
AS_IF(test "x${ac_cv_header_memcached_engine_h}" != "xyes",
      AC_MSG_ERROR([Can't build bucket_engine without memcached/engine.h]))

AC_SEARCH_LIBS(dlopen, dl)
AC_SEARCH_LIBS(log, m)

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile management/python_wrapper)
AC_OUTPUT
