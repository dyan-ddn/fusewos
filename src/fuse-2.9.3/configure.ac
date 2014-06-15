AC_INIT(fuse, 2.9.3)

AC_PREREQ(2.59d)
AC_CONFIG_MACRO_DIR([m4])
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES(yes)])
AC_CONFIG_HEADERS(include/config.h)

AC_PROG_LIBTOOL
AC_PROG_CC
AC_PROG_MKDIR_P
AM_PROG_CC_C_O

case $target_os in
     *linux*)	arch=linux;;
     *netbsd*)  arch=netbsd;;
     *bsd*)	arch=bsd;;
     *)		arch=unknown;;
esac

if test "$ac_env_CFLAGS_set" != set; then
	CFLAGS="-Wall -W -Wno-sign-compare -Wstrict-prototypes -Wmissing-declarations -Wwrite-strings -g -O2 -fno-strict-aliasing"
fi

AC_ARG_ENABLE(lib,
	[  --enable-lib            Compile with library ])
AC_ARG_ENABLE(util,
	[  --enable-util           Compile with util ])
AC_ARG_ENABLE(example,
	[  --enable-example        Compile with examples ])
AC_ARG_ENABLE(mtab,
	[  --disable-mtab          Disable and ignore usage of /etc/mtab ])

AC_ARG_WITH(pkgconfigdir,
            [  --with-pkgconfigdir=DIR      pkgconfig file in DIR @<:@LIBDIR/pkgconfig@:>@],
            [pkgconfigdir=$withval],
            [pkgconfigdir='${libdir}/pkgconfig'])
AC_SUBST(pkgconfigdir)

subdirs2="include"

if test "$enable_lib" != "no"; then
	subdirs2="$subdirs2 lib";
fi
if test "$arch" = linux -a "$enable_util" != "no"; then
	subdirs2="$subdirs2 util";
fi
if test "$enable_example" != "no"; then
	subdirs2="$subdirs2 example";
fi
if test "$enable_mtab" = "no"; then
	AC_DEFINE(IGNORE_MTAB, 1, [Don't update /etc/mtab])
fi

AC_CHECK_FUNCS([fork setxattr fdatasync splice vmsplice utimensat])
AC_CHECK_FUNCS([posix_fallocate])
AC_CHECK_MEMBERS([struct stat.st_atim])
AC_CHECK_MEMBERS([struct stat.st_atimespec])

LIBS=
AC_SEARCH_LIBS(dlopen, [dl])
AC_SEARCH_LIBS(clock_gettime, [rt])
libfuse_libs=$LIBS
LIBS=
AC_ARG_WITH([libiconv-prefix],
[  --with-libiconv-prefix=DIR  search for libiconv in DIR/include and DIR/lib], [
    for dir in `echo "$withval" | tr : ' '`; do
      if test -d $dir/include; then CPPFLAGS="$CPPFLAGS -I$dir/include"; fi
      if test -d $dir/lib; then LDFLAGS="$LDFLAGS -L$dir/lib"; fi
    done
   ])
AM_ICONV
libfuse_libs="$libfuse_libs $LTLIBICONV"
AM_CONDITIONAL(ICONV, test "$am_cv_func_iconv" = yes)
AC_SUBST(libfuse_libs)

if test -z "$MOUNT_FUSE_PATH"; then
	MOUNT_FUSE_PATH=/sbin
fi
AC_SUBST(MOUNT_FUSE_PATH)
if test -z "$UDEV_RULES_PATH"; then
	UDEV_RULES_PATH=/etc/udev/rules.d
fi
AC_SUBST(UDEV_RULES_PATH)
if test -z "$INIT_D_PATH"; then
	INIT_D_PATH=/etc/init.d
fi
AC_SUBST(INIT_D_PATH)

AC_SUBST(subdirs2)

AM_CONDITIONAL(LINUX, test "$arch" = linux)
AM_CONDITIONAL(NETBSD, test "$arch" = netbsd)
AM_CONDITIONAL(BSD, test "$arch" = bsd)

util_linux_ok=yes
if test "$arch" = linux -a "$cross_compiling" != "yes"; then
	AC_MSG_CHECKING([if umount supports --fake --no-canonicalize])
	# exit code of umount is 1 if option is unrecognised, 2 otherwise
	umount --fake --no-canonicalize > /dev/null 2>&1
	if test $? != 1; then
		AC_MSG_RESULT([yes])
	else
		firstline=`umount --fake --no-canonicalize 2>&1 | head -1`
		if test "$firstline" = 'umount: only root can use "--fake" option'; then
			AC_MSG_RESULT([yes])
		else
			AC_MSG_RESULT([$firstline])
			util_linux_ok=no
		fi
	fi
fi

# this is example-file: configure.ac

# initial information about the project
#AC_INIT([myProg],[2.0],[me@myProg.org])

# check if the source folder is available
#AC_CONFIG_SRCDIR([example/fusewos.cpp])

# check for C++ preprocessor and compiler
#AC_PROG_CXXCPP
#AC_PROG_CXX

# automake initialization (mandatory) including a check for automake API version >= 1.9
#AM_INIT_AUTOMAKE([1.9])

# use the C++ compiler for the following checks
#AC_LANG([C++])

# Checks for header files.
#AC_HEADER_STDC
#AC_CHECK_HEADERS([string])
#AC_CHECK_HEADERS([iostream])

# Checks for typedefs, structures, and compiler characteristics.
#AC_TYPE_SIZE_T

# distribute additional compiler and linker flags among Makefiles
# --> set and change these variables instead of CXXFLAGS or LDFLAGS (for user only)
#AC_SUBST([AM_CXXFLAGS])
#AC_SUBST([AM_LDFLAGS])

# files to generate via autotools (prepare .am or .in source files)
#AC_CONFIG_FILES([Makefile])
#AC_CONFIG_FILES([src/Makefile])

# finally this generates the Makefiles etc. for the build
#AC_OUTPUT

AC_CONFIG_FILES([fuse.pc Makefile lib/Makefile util/Makefile example/Makefile include/Makefile doc/Makefile])
AC_OUTPUT

if test "$util_linux_ok" = no; then
	AC_MSG_WARN([
******************************************************************
* Please install util-linux version 2.18 or later which supports *
* --fake and --no-canonicalize options in mount and umount       *
******************************************************************])
fi

