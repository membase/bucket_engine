AC_PREREQ(2.52)
AC_INIT(bucket_engine, 0.1, dustin@spy.net)
AC_CANONICAL_SYSTEM
AC_CONFIG_SRCDIR(bucket_engine.c)
AM_INIT_AUTOMAKE(AC_PACKAGE_NAME, AC_PACKAGE_VERSION)
AM_CONFIG_HEADER(config.h)

AC_PROG_CC
AM_PROG_CC_C_O
AC_DISABLE_STATIC
AC_PROG_LIBTOOL

AC_SEARCH_LIBS(dlopen, dl)

AC_ARG_ENABLE(coverage,
  [AS_HELP_STRING([--enable-coverage], [Enable code coverage])])

if test "x$enable_coverage" = "xyes"; then
   if test "$ICC" = "yes"
   then
      dnl ICC trying to be gcc, but not well
      CFLAGS="$CFLAGS -pthread"
   elif test "$GCC" = "yes"
   then
      CFLAGS="$CFLAGS -pthread"
      AC_PATH_PROG([PROFILER], [gcov], "no", [$PATH])
      if test "x$PROFILER" != "xno"; then
         AC_CHECK_LIB(gcov, main,
                    [
                      PROFILER_FLAGS="-fprofile-arcs -ftest-coverage"
                      PROFILER_LDFLAGS="-lgcov"
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

CPPFLAGS="-I\${top_srcdir}/memcached/include ${CPPFLAGS}"
AC_CONFIG_FILES(Makefile)
AC_OUTPUT
