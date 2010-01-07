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
                        trymemcacheddir=$withval
                fi
       ]
)

dnl ----------------------------------------------------------------------------

AC_CACHE_CHECK([for memcached directory], ac_cv_memcached_dir, [
  saved_CPPFLAGS="$CPPFLAGS"
  mc_found=no
  for mcdir in $trymemcacheddir "" $prefix /usr/local ; do
    # Skip the directory if it isn't there.
    if test ! -z "$mcdir" -a ! -d "$mcdir" ; then
       continue;
    fi
    if test ! -z "$mcdir" ; then
      if test -d "$mcdir/include" ; then
        CPPFLAGS="-I$mcdir/include $CPPFLAGS"
      else
        CPPFLAGS="-I$mcdir $CPPFLAGS"
      fi
    fi
    # Can I compile and link it?
    AC_TRY_LINK([#include <memcached/engine.h>], [ ],
       [ memcached_linked=yes ], [ memcached_linked=no ])
    if test $memcached_linked = yes; then
       if test ! -z "$mcdir" ; then
         ac_cv_memcached_dir=$mcdir
       else
         ac_cv_memcached_dir="(system)"
       fi
       mc_found=yes
       break
    fi
  done
  CPPFLAGS="$saved_CPPFLAGS"
  if test $mc_found = no ; then
    AC_MSG_ERROR([memcached is required.

      If it's already installed, specify its path using --with-memcached=/dir/
])
  fi
])
if test $ac_cv_memcached_dir != "(system)"; then
  if test -d "$ac_cv_memcached_dir/lib" ; then
    LDFLAGS="-L$ac_cv_memcached_dir/lib $LDFLAGS"
    mc_libdir="$ac_cv_memcached_dir/lib"
  else
    LDFLAGS="-L$ac_cv_memcached_dir $LDFLAGS"
    mc_libdir="$ac_cv_memcached_dir"
  fi
  if test -d "$ac_cv_memcached_dir/include" ; then
    CPPFLAGS="-I$ac_cv_memcached_dir/include $CPPFLAGS"
  else
    CPPFLAGS="-I$ac_cv_memcached_dir $CPPFLAGS"
  fi
fi

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile)
AC_OUTPUT
