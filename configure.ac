AC_PREREQ(2.52)
AC_INIT(bucket_engine, 0.1, dustin@spy.net)
AC_CANONICAL_SYSTEM
AC_CONFIG_SRCDIR(bucket_engine.c)
AM_INIT_AUTOMAKE(AC_PACKAGE_NAME, AC_PACKAGE_VERSION)
AM_CONFIG_HEADER(config.h)

AC_PROG_CC
AC_PROG_CC_C99
AM_PROG_CC_C_O
AC_DISABLE_STATIC
AC_PROG_LIBTOOL

AC_SEARCH_LIBS(dlopen, dl)
AC_SEARCH_LIBS(log, m)

AC_ARG_ENABLE(coverage,
  [AS_HELP_STRING([--enable-coverage], [Enable code coverage])])

if test "x$enable_coverage" = "xyes"; then
   if test "$ICC" = "yes"
   then
      dnl ICC trying to be gcc, but not well
      CFLAGS="$CFLAGS -Wall -pthread"
   elif test "$GCC" = "yes"
   then
      CFLAGS="$CFLAGS -Wall -pthread"
      AC_PATH_PROG([PROFILER], [gcov], "no", [$PATH])
      if test "x$PROFILER" != "xno"; then
         AC_CHECK_LIB(gcov, main,
                    [
                      PROFILER_FLAGS="-fprofile-arcs -ftest-coverage"
                      PROFILER_LDFLAGS="-lgcov"
                      CFLAGS="$CFLAGS -DUSE_GCOV=1"
                    ], [
                      PROFILER_FLAGS=
                      PROFILER_LDFLAGS=
                    ])
      fi
   elif test "$SUNCC" = "yes"
   then
      AC_PATH_PROG([PROFILER], [tcov], "no", [$PATH])
      if test "x$PROFILER" != "xno"; then
         PROFILER_FLAGS=-xprofile=tcov
      fi
   fi
fi
AC_SUBST(PROFILER_FLAGS)
AC_SUBST(PROFILER_LDFLAGS)

trymemcacheddir=""
AC_ARG_WITH(memcached,
       [  --with-memcached=PATH     Specify path to memcached installation ],
       [
                if test "x$withval" != "xno" ; then
                        memcacheddir=$withval
                fi
       ]
)

dnl ----------------------------------------------------------------------------

AC_CACHE_CHECK([for memcached directory], ac_cv_memcached_dir, [
  if ! test -f "$memcacheddir/libmcd_util.la"
  then
    AC_ERROR([Could not find $memcacheddir/libmcd_util.la])
  fi
  ac_cv_memcached_dir=$memcacheddir
])

MEMCACHED_DIR=$ac_cv_memcached_dir
AC_SUBST(MEMCACHED_DIR)


dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile)
AC_OUTPUT
