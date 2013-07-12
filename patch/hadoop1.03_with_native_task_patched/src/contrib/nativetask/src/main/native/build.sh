#!/bin/sh

SCRIPT_PATH="${BASH_SOURCE[0]}";
if ([ -h "${SCRIPT_PATH}" ]) then
  while([ -h "${SCRIPT_PATH}" ]) do SCRIPT_PATH=`readlink "${SCRIPT_PATH}"`; done
fi
pushd . > /dev/null
cd `dirname ${SCRIPT_PATH}` > /dev/null
SCRIPT_PATH=`pwd`;

export AUTOM4TE="autom4te"
export AUTOCONF="autoconf"


init() {
        set -x
        aclocal
        libtoolize --force --copy
        autoconf --force
        automake --foreign --copy --add-missing
}

clean() {
        echo 'cleaning...'
        make distclean >/dev/null 2>&1
        rm -rf autom4te.cache m4
        for fn in mkinstalldirs config.log config.status Makefile simple.sh config.h.in~; do
                rm -f $fn
        done

        find . -name Makefile.in -exec rm -f {} \;
        find . -name Makefile -exec rm -f {} \;
        find . -name .deps -prune -exec rm -rf {} \;
        echo 'done'
}

install() {
  target=$1
  targetlibs=$target/.libs
  echo "target folder: "$targetlibs
  rm -rf  $targetlibs
  mkdir -p $targetlibs
  cp -r .libs $target
}



build() {
if hash icpc 2>/dev/null; then
echo "We are going to use Intel compiler icpc"
.$basedir/configure CC="icc -static-intel" CXX="icpc -static-intel"
else
.$basedir/configure
fi
automake
make
}


case "x$1" in
xclean)
	clean
	;;
xinit)
	
;;	
xall)
        clean
	build
	;;
xinstall)
	install $2
esac


popd  > /dev/null
