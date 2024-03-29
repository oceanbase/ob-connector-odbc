dnl Process this file with autoconf to produce a configure script.

AC_INIT([unixODBC], [2.3.7], nick@unixodbc.org, [unixODBC])
AM_INIT_AUTOMAKE

dnl Checks for programs.
AC_PROG_AWK
AC_PROG_YACC
AC_PROG_CPP
AC_PROG_CC
AM_PROG_LEX
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_CONFIG_MACRO_DIR([m4])

AC_DISABLE_STATIC

dnl Check if we want to worry about threads

AC_ARG_ENABLE( threads, 
[  --enable-threads        build with thread support [default=yes]],
[ case "${enableval}" in
    yes) thread=true ;;
    no) thread=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-thread) ;;
    esac],[thread=true])

AC_ARG_ENABLE( gnuthreads, 
[  --enable-gnuthreads     build with gnu threads support [default=no]],
[ case "${enableval}" in
    yes) gnuthread=true ;;
    no) gnuthread=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-gnuthread) ;;
    esac],[gnuthread=false])

AC_ARG_ENABLE( readline, 
[  --enable-readline       build with readline  support [default=yes]],
[ case "${enableval}" in
    yes) readline=true ;;
    no) readline=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-readline) ;;
    esac],[readline=true])

AC_ARG_ENABLE( inicaching, 
[  --enable-inicaching     build with ini file caching  support [default=yes]],
[ case "${enableval}" in
    yes) inicaching=true ;;
    no) inicaching=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-inicaching) ;;
    esac],[inicaching=true])

dnl Check if we want to build the drivers

AC_ARG_ENABLE( drivers, 
[  --enable-drivers        build included drivers [default=no]],
[ case "${enableval}" in
    yes) drivers=true ;;
    no) drivers=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-drivers) ;;
    esac],[drivers=false])

dnl Check if we want to build the driver config

AC_ARG_ENABLE( driverc, 
[  --enable-driver-conf    build included driver config libs [default=no]],
[ case "${enableval}" in
    yes) driverc=true ;;
    no) driverc=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-driver-conf) ;;
    esac],[driverc=false])

AC_ARG_ENABLE( fastvalidate, 
[  --enable-fastvalidate   use relaxed handle checking in the DM [default=no]],
[ case "${enableval}" in
    yes) fastvalidate=true ;;
    no) fastvalidate=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-fastvalidate) ;;
    esac],[fastvalidate=false])

AC_ARG_ENABLE( iconv, 
[  --enable-iconv          build with iconv support [default=yes]],
[ case "${enableval}" in
    yes) iconv=true ;;
    no) iconv=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-iconv) ;;
    esac],[iconv=true])

dnl Check for sys/sem.h

AC_CHECK_HEADERS(sys/sem.h, semh=true, semh=false)

AC_ARG_ENABLE( stats,
[  --enable-stats          build with statistic gathering support [default=no]],
[ case "${enableval}" in
    yes) if test "x$semh" = "xfalse"; then
           AC_MSG_ERROR(stats enabled but required header was not found)
         fi
        stats=true ;;
    no) stats=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-stats) ;;
    esac],[stats=false])

stats_ftok_name="odbc.ini"

AC_ARG_WITH(stats_ftok_name,
    [  --with-stats-ftok-name=filename  Filename to get IPC ID for stats gathering  [default=odbc.ini]],
       stats_ftok_name="$withval"
    )

STATS_FTOK_NAME="$stats_ftok_name"

AC_SUBST(STATS_FTOK_NAME)

AC_DEFINE_UNQUOTED([STATS_FTOK_NAME],"$STATS_FTOK_NAME",[Filename to use for ftok])

AC_ARG_ENABLE( setlibversion,
[  --enable-setlibversion  build with VERS set in libraries [default=no]],
[ case "${enableval}" in
    yes) setlibversion=true ;;
    no) setlibversion=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-setlibversion) ;;
    esac],[setlibversion=false])

AC_ARG_ENABLE( handlemap, 
[  --enable-handlemap      driver manager can map driver handles called back from broken drivers [default=no]],
[ case "${enableval}" in
    yes) handlemap=true ;;
    no) handlemap=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-handlemap) ;;
    esac],[handlemap=false])

AC_ARG_ENABLE( stricterror, 
[  --enable-stricterror    error conform to the MS spec, the unixODBC prefix is removed for driver errors [default=yes]],
[ case "${enableval}" in
    yes) stricterror=true ;;
    no) stricterror=false ;;
    *) AC_MSG_ERROR(bad value ${enableval} for --enable-stricterror) ;;
    esac],[stricterror=true])

AC_ARG_ENABLE( gui, 
[  --enable-gui            only included for backwards compatibility, gui now in its own project, see ChangeLog],
[ case "${enableval}" in
    *) ;;
    esac])


dnl Enable building of the convenience library
dnl and set LIBLTDL accordingly

CFLAGS="-z noexecstack -z now -pie -fPIC -fstack-protector"
LDFLAGS="-Wl,-z,relro,-z,now "
LIBS="-Wl,-z,relro,-z,now "

INCLTDL=""
LIBLTDL=""

AC_LIBTOOL_WIN32_DLL
dnl AC_PROG_LIBTOOL

dnl AC_CONFIG_MACRO_DIR([libltdl/m4])
dnl LT_CONFIG_LTDL_DIR([libltdl])
dnl LTDL_INIT

LT_CONFIG_LTDL_DIR([libltdl])
LT_INIT([dlopen])
LTDL_INIT([convenience])

dnl Substitute INCLTDL and LIBLTDL in the Makefiles
AC_SUBST(LTDLINCL)
AC_SUBST(LIBLTDL)

#dnl Find shared lib extension
#AC_MSG_CHECKING(for shared lib extension)
#SHLIBEXT="$shrext_cmds"
#AC_MSG_RESULT($shrext_cmds)
#AC_SUBST(SHLIBEXT)

dnl Find shared lib extension
AC_MSG_CHECKING(for shared lib extension)
eval "SHLIBEXT=$shrext_cmds"
AC_MSG_RESULT($SHLIBEXT)
AC_SUBST(SHLIBEXT,$SHLIBEXT)

AC_DEFINE_UNQUOTED([SHLIBEXT], "$shrext_cmds", [Shared lib extension])
AC_DEFINE_DIR([DEFLIB_PATH], [libdir], [Lib directory])
AC_DEFINE_DIR([LIB_PREFIX], [libdir], [Lib directory])
AC_DEFINE_DIR([SYSTEM_FILE_PATH], [sysconfdir], [System file path])
AC_DEFINE_DIR([SYSTEM_LIB_PATH], [libdir], [Lib path])
AC_DEFINE_DIR([PREFIX], [prefix], [Install prefix])
AC_DEFINE_DIR([EXEC_PREFIX], [exec_prefix], [Install exec_prefix])
AC_DEFINE_DIR([BIN_PREFIX], [bindir], [Install bindir])
AC_DEFINE_DIR([INCLUDE_PREFIX], [includedir], [Install includedir])
AC_DEFINE([UNIXODBC], [], [Flag that we are not using another DM])

if test "x$iconv" = "xtrue"; 
then

AM_ICONV

iconv_char_enc="auto-search"
AC_ARG_WITH(iconv_char_enc,
    [  --with-iconv-char-enc=enc   Encoding to use as ASCII [default=auto-search] ],
       iconv_char_enc="$withval"
    )

ICONV_CHAR_ENCODING="$iconv_char_enc"

iconv_ucode_enc="auto-search"

AC_ARG_WITH(iconv_ucode_enc,
    [  --with-iconv-ucode-enc=enc  Encoding to use as UNICODE [default=auto-search] ],
       iconv_ucode_enc="$withval"
    )

ICONV_CHAR_ENCODING=""
ICONV_UNICODE_ENCODING=""

if test "$am_cv_func_iconv" = yes; then
  AC_MSG_CHECKING( for encoding to use for CHAR representations );
  ICONV_CHAR_ENCODING="$iconv_char_enc"
  AC_MSG_RESULT( $iconv_char_enc );

  AC_MSG_CHECKING( for encoding to use for UNICODE representations );
  ICONV_UNICODE_ENCODING="$iconv_ucode_enc"
  AC_MSG_RESULT( $iconv_ucode_enc );
fi

AC_SUBST(ICONV_CHAR_ENCODING)
AC_SUBST(ICONV_UNICODE_ENCODING)

AC_DEFINE_UNQUOTED([UNICODE_ENCODING],"$ICONV_UNICODE_ENCODING",[Encoding to use for UNICODE])
AC_DEFINE_UNQUOTED([ASCII_ENCODING],"$ICONV_CHAR_ENCODING",[Encoding to use for CHAR])

fi

dnl Checks for libraries.
AC_CHECK_LIB(crypt, crypt, [ AC_DEFINE([HAVE_LIBCRYPT], [], [Add -lcrypt to lib list]) LIBADD_CRYPT="-lcrypt" ])
AC_SUBST(LIBADD_CRYPT)
AC_CHECK_LIB(m, pow, [ LIBADD_POW="-lm" ], )
AC_SUBST(LIBADD_POW)

have_readline="no"

if test "x$readline" = "xtrue"; then
  AC_CHECK_LIB_NOC(readline, readline, 
  [
      READLINE=-lreadline
      have_readline="yes"
  ],
  [
      dnl try with -lcurses
      AC_CHECK_LIB_NOC(readline, readline,
      [
          READLINE="-lreadline -lcurses"
          have_readline="yes"
      ],
      [
      ],
      -lcurses )
  ])

  if test "x$have_readline" = "xyes"; then
      AC_CHECK_HEADERS(readline/history.h, [AC_DEFINE([HAVE_READLINE], [1], [Add readline support])])
  fi
fi

AC_SUBST(READLINE)

AC_MSG_CHECKING( Are we using ini caching )
if test "x$inicaching" = "xtrue"; then
	AC_DEFINE([ENABLE_INI_CACHING],[],[Using ini cacheing])
	AC_MSG_RESULT( yes );
else
	AC_MSG_RESULT( no );
fi

dnl Are we using flex
if test "x$drivers" = "xtrue"; then
AC_MSG_CHECKING( Are we using flex )
if test "x$LEX" = "xflex"; then
LFLAGS="$LFLAGS -i"
AC_MSG_RESULT( yes );
AC_CHECK_LIB(c, scandir, [AC_DEFINE([HAVE_SCANDIR], [1], [Use the scandir lib])] )
else
AC_MSG_RESULT( no - text driver disabled );
fi
AM_CONDITIONAL(HAVE_FLEX, test "x$LEX" = "xflex" )
AC_SUBST(LFLAGS)
else
AM_CONDITIONAL(HAVE_FLEX, test "xabc" = "xdef" )
fi

case $host_os in
    *qnx* )
    qnx="true"
    AC_DEFINE([QNX_LIBLTDL],[],[Using QNX])
    ;;
esac

dnl check how time() can be used
AC_HEADER_TIME
AC_CHECK_HEADERS(sys/time.h)

AC_CHECK_SIZEOF(long, 4)

AC_MSG_CHECKING([if platform is 64 bit])
if test "$ac_cv_sizeof_long" = "8"; then
	AC_MSG_RESULT( Yes );
    AC_DEFINE([PLATFORM64],[],[Platform is 64 bit])
else
	AC_MSG_RESULT( No );
fi

AC_CHECK_LONG_LONG
AC_CHECK_SIZEOF([long int])
AC_CHECK_TYPES([ptrdiff_t])

AC_CHECK_FUNCS( strcasecmp strncasecmp vsnprintf strtol atoll strtoll endpwent gettimeofday ftime time stricmp strnicmp getuid getpwuid nl_langinfo )

AM_LANGINFO_CODESET

LIBADD_DL=
AC_SUBST(LIBADD_DL)

THREADLIB=""
if test "x$thread" = "xtrue"; then
  if test "x$gnuthread" = "xtrue"; then
    AC_CHECK_PTH( 1.3.0 )
    CPPFLAGS="$CPPFLAGS $PTH_CPPFLAGS"
    CFLAGS="$CFLAGS $PTH_CFLAGS"
    LDFLAGS="$LDFLAGS $PTH_LDFLAGS"
    THREADLIB="$PTH_LIBS"
	AC_DEFINE([HAVE_LIBPTH], [1], [Use the -lpth thread library])
  else
    gotthread="no";

	AC_MSG_CHECKING( if os is AIX )
	case $host_os in
    	"aix"*)
			raw_threads="no";
			AC_MSG_RESULT( yes - disable check for libthread );
    	;;
    	*)
			raw_threads="yes";
			AC_MSG_RESULT( no - enable check for libthread );
        ;;
	esac

    if test "x$raw_threads" = "xyes"; then
    	AC_CHECK_LIB_NOC(thread, mutex_lock,
    	[
		AC_DEFINE([HAVE_LIBTHREAD], [1], [Use the -lthread threading lib])
      	dnl Check if the compiler will build with -mt as a option, this is a solaris thing
      	AC_CHECK_COMP_OPT(mt)
      	gotthread="yes";
      	THREADLIB="-lthread"
    	])
	fi

    if test "x$gotthread" = "xno"; then
      AC_CHECK_LIBPT_NOC(pthread, pthread_mutex_lock, 
      [
		AC_DEFINE([HAVE_LIBPTHREAD], [1], [Use -lpthread threading lib])
        gotthread="yes";
        THREADLIB="-lpthread"
        if test "x$ac_cv_prog_gcc" = "xyes"; then
          dnl Check if the compiler will build with -pthread as a option
          AC_CHECK_COMP_OPT(pthread)
        else
          dnl Check if the compiler will build with -mt as a option
          AC_CHECK_COMP_OPT(mt)
        fi
      ])
    fi

    if test "x$gotthread" = "xno"; then
      AC_CHECK_LIBPT_NOC(c, pthread_mutex_lock, 
      [
        AC_DEFINE(HAVE_LIBPTHREAD,1)
        gotthread="yes";
        THREADLIB=""
        if test "x$ac_cv_prog_gcc" = "xyes"; then
          dnl Check if the compiler will build with -pthread as a option
          AC_CHECK_COMP_OPT(pthread)
        else
          dnl Check if the compiler will build with -mt as a option
          AC_CHECK_COMP_OPT(mt)
        fi
      ])
    fi

    if test "x$gotthread" = "xno"; then
      if test "x$ac_cv_prog_gcc" = "xyes"; then
        dnl This is for freebsd that needs -lpthread before it finds the lib
        AC_CHECK_COMP_OPT(pthread)
        AC_CHECK_LIBPT_NOC(c, pthread_mutex_lock,
        [
          AC_DEFINE(HAVE_LIBPTHREAD,1)
          THREADLIB="-pthread -lc_r"
          gotthread="yes";
        ])
      fi
    fi

    dnl Check for AIX 
    if test "x$gotthread" = "xno"; then
      SAVECFLAGS="$CFLAGS"
      CFLAGS="$CFLAGS -D_THREAD_SAFE -D_ALL_SOURCE -D_LONG_LONG"
      AC_CHECK_LIBPT_NOC(pthread, pthread_mutex_lock, 
      [
        AC_DEFINE(HAVE_LIBPTHREAD,1)
        gotthread="yes";
        THREADLIB="-lpthread"
      ])
      CFLAGS="$SAVECFLAGS"
      AC_DEFINE([_THREAD_SAFE],[],[Build flag for AIX])
      AC_DEFINE([_ALL_SOURCE],[],[Build flag for AIX])
      AC_DEFINE([_LONG_LONG],[],[Build flag for AIX])
    fi

    if test "x$gotthread" = "xyes"; then
      dnl do not add a -lc because of this
      save_LIBS=$LIBS
      AC_CHECK_LIB(c, localtime_r, [AC_DEFINE([HAVE_LOCALTIME_R], [1], [Use rentrant version of localtime] )])
      LIBS=$save_LIBS
    fi
  fi
fi

case $host_os in
    "darwin"*)
        stats="false"
        macosx="yes"
        AC_DEFINE([OSXHEADER],[],[Using OSX])
    ;;

    sysv5Open*)
        if test "x$THREADLIB" = "x"; then
          LIBS="$LIBS $THREADLIB"
        else
          LIBS="$LIBS -Kthread"
        fi
        ;;

    *)
        LIBS="$LIBS $THREADLIB"
        ;;
esac

if test "x$stats" = "xtrue"; then
  AC_CHECK_FUNCS( ftok semget shmget semop snprintf,[],[stats=false])
fi

if test "x$stats" = "xtrue"; then
AC_CHECK_SEMUNDOO
AC_DEFINE([COLLECT_STATS], [], [Use a semaphore to allow ODBCConfig to display running counts])
fi

AC_ARG_WITH(msql-lib,
    [  --with-msql-lib=DIR     where the root of MiniSQL libs are installed ],
       msql_libraries="$withval"
    )

AC_ARG_WITH(msql-include,
    [  --with-msql-include=DIR where the MiniSQL headers are installed ],
       msql_headers="$withval"
    )

AC_SUBST(msql_libraries)
AC_SUBST(msql_headers)

dnl Checks for header files.
AC_HEADER_STDC

AC_CHECK_HEADERS(malloc.h unistd.h pwd.h crypt.h limits.h synch.h strings.h string.h locale.h sys/malloc.h sys/types.h sys/sem.h stdarg.h varargs.h sys/time.h sys/timeb.h time.h langinfo.h stddef.h )

INCLUDES="$INCLUDES $USER_INCLUDES";

dnl only build the mSQL code if the headers are in place
AC_CHECK_HEADERS(msql.h,[msql=true],
[
   msql=false;
   for ac_dir in $kde_extra_includes $msql_headers; 
   do
    AC_CHECK_HEADERS( $ac_dir/msql.h,
    [
        msql=true;
        INCLUDES="$INCLUDES $USER_INCLUDES -I$ac_dir";
    ])
   done
])

dnl AC_SUBST(all_includes)
dnl AC_SUBST(all_libraries)

AM_CONDITIONAL(MSQL, test "x$msql" = "xtrue" )
AM_CONDITIONAL(DRIVERS, test "x$drivers" = "xtrue" )
AM_CONDITIONAL(DRIVERC, test "x$driverc" = "xtrue" )
AM_CONDITIONAL(QNX, test "x$qnx" = "xtrue" )
AM_CONDITIONAL(WITHLT, test "x$use_builtin_libtool" = "xyes" )

dnl This blows up due to what I think is a bug in automake 1.6.3
dnl AC_SUBST(INCLUDES)

if test "x$fastvalidate" = "xtrue"; then
AC_DEFINE([FAST_HANDLE_VALIDATE], [], [Disable the precise but slow checking of the validity of handles])
fi

if test "x$handlemap" = "xtrue"; then
AC_DEFINE([WITH_HANDLE_REDIRECT],[],[Work with IBM drivers that use 32 bit handles on 64 bit platforms])
fi

if test "x$stricterror" = "xtrue"; then
AC_DEFINE([STRICT_ODBC_ERROR],[],[don't include unixODBC prefix in driver error messages])
fi

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T
AC_STRUCT_TM
AC_TYPE_UID_T
AC_HEADER_DIRENT

dnl Checks for library functions.
AC_FUNC_ALLOCA
AC_FUNC_VPRINTF
AC_CHECK_FUNCS( putenv socket strdup strstr setenv setlocale strchr )

dnl This is the unixODBC source tree
AC_DEFINE([UNIXODBC_SOURCE],[],[We are building inside the unixODBC source tree])

LIB_VERSION="2:8:0"
AC_SUBST(LIB_VERSION)

AC_CONFIG_HEADERS(config.h)
AC_CONFIG_HEADERS(unixodbc_conf.h)

AC_CONFIG_FILES([
  Makefile
  extras/Makefile
  log/Makefile
  lst/Makefile
  ini/Makefile
  odbcinst/Makefile
  odbcinst/odbcinst.pc
  cur/Makefile
  cur/odbccr.pc
  DriverManager/Makefile
  DriverManager/odbc.pc
  exe/Makefile
  DRVConfig/Makefile
  DRVConfig/drvcfg1/Makefile
  DRVConfig/drvcfg2/Makefile
  DRVConfig/PostgreSQL/Makefile
  DRVConfig/MiniSQL/Makefile
  DRVConfig/MySQL/Makefile
  DRVConfig/nn/Makefile
  DRVConfig/esoob/Makefile
  DRVConfig/oplodbc/Makefile
  DRVConfig/template/Makefile
  DRVConfig/tds/Makefile
  DRVConfig/txt/Makefile
  DRVConfig/Oracle/Makefile
  DRVConfig/sapdb/Makefile
  DRVConfig/Mimer/Makefile
  Drivers/Makefile
  Drivers/Postgre7.1/Makefile
  Drivers/nn/Makefile
  Drivers/template/Makefile
  Drivers/MiniSQL/Makefile
  include/Makefile
  man/Makefile
  doc/Makefile
  doc/AdministratorManual/Makefile
  doc/ProgrammerManual/Makefile
  doc/ProgrammerManual/Tutorial/Makefile
  doc/UserManual/Makefile
  doc/lst/Makefile
  samples/Makefile
])
AC_OUTPUT

dnl Attempt to add version information to libraries generated by libtool
AC_MSG_CHECKING( are we setting library version )
if test "x$setlibversion" = "xtrue"; then
	AC_MSG_RESULT( yes );
    sed '/archive_expsym_cmds=/s/{ global/VERS_3.52 {global/' < libtool > libtool.tmp; mv libtool.tmp libtool
else
	AC_MSG_RESULT( no );
fi
