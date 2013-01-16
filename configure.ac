# bucket_engine
# Copyright (C) 2010, 2011, 2012 Couchbase, Inc
# All rights reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(bucket_engine, VERSION_NUMBER, support@couchbase.com)
AC_CONFIG_SRCDIR([bucket_engine.c])
AC_CONFIG_AUX_DIR(config)
AC_USE_SYSTEM_EXTENSIONS
AM_INIT_AUTOMAKE
AC_PROG_CC
AM_PROG_CC_C_O
AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# the malloc tests seems to be broken for cross compilation.. ignore them
ac_cv_func_malloc_0_nonnull=yes
ac_cv_func_realloc_0_nonnull=yes

COUCHBASE_GENERIC_COMPILER

AC_CHECK_HEADERS([atomic.h])

AC_ARG_WITH([memcached],
    [AS_HELP_STRING([--with-memcached],
      [Memcached source location])],
    [ac_cv_with_memcached="$withval"],
    [ac_cv_with_memcached=""])

AC_ARG_WITH([management],
    [AS_HELP_STRING([--with-management],
      [Build and install management scripts @<:@default=no@:>@])],
    [ac_cv_with_management="$withval"],
    [ac_cv_with_management="no"])

AS_IF(test "x${ac_cv_with_memcached}" != "x",
      [AM_CPPFLAGS="-I${ac_cv_with_memcached}/include"
       PATH="${ac_cv_with_memcached}:$PATH"])

memcached_headers_save_CPPFLAGS=$CPPFLAGS
CPPFLAGS="$AM_CPPFLAGS $memcached_headers_save_CPPFLAGS"
AC_CHECK_HEADERS(memcached/engine.h)
CPPFLAGS="$memcached_headers_save_CPPFLAGS"
AS_IF(test "x${ac_cv_header_memcached_engine_h}" != "xyes",
      AC_MSG_ERROR([Can't build bucket_engine without memcached/engine.h]))

AC_CHECK_HEADERS(ep-engine/command_ids.h)
AS_IF(test "x${ac_cv_header_ep_engine_command_ids_h}" != "xyes",
      AC_MSG_ERROR([Can't build bucket_engine without ep-engine/command_ids.h]))

AM_CONDITIONAL(BUILD_MANAGEMENT, [test x"$ac_cv_with_management" != "xno"])

AC_SEARCH_LIBS(pthread_mutex_init, pthread)
AC_SEARCH_LIBS(dlopen, dl)
AC_SEARCH_LIBS(log, m)

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile python_wrapper)
AC_OUTPUT
