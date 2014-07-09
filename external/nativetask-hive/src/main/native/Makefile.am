export PLATFORM = `uname | tr [A-Z] [a-z]`
#export BITS = `sh ./getosbits.sh`
export BITS = 64

ACLOCAL_AMFLAGS = -I m4

SNAPPY_PREFIX ?= $(HOME)/local

if PLATFORM_IS_MACOS
# macos 
JNI_INCLUDE ?= -I/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers/
PLATFORM_LDFLAGS = -framework JavaVM
MACRO = -D__X$(BITS)
endif

if PLATFORM_IS_LINUX
# linux
JNI_INCLUDE ?= -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
PLATFORM_LDFLAGS = -lrt -L$(JAVA_HOME)/jre/lib/amd64/server -ljvm
MACRO = -D__X$(BITS)
endif

if PLATFORM_IS_WIN
# windows
JNI_INCLUDE ?= -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/win32
PLATFORM_LDFLAGS = -lrt   -L$(JAVA_HOME)/lib -ljvm -Wl,--add-stdcall-alias -mno-cygwin
MACRO = -D__X$(BITS) -D__int64="long long" -D__CYGWIN__  -DSOFTWARE_CRC
endif

# primatives configs 
PRFLAGS=-DSIMPLE_MEMCPY 
#-DSPILLRECORD_CHECKSUM_UINT

NATIVETASK = $(top_srcdir)/../../../../../core/src/main/native
NATIVETASK_FLAGS = -I$(NATIVETASK)/src -I$(NATIVETASK)/src/lib -I$(NATIVETASK)/src/util -I$(NATIVETASK)/test
AM_LDFLAGS = -no-undefined -L$(SNAPPY_PREFIX)/lib -lsnappy -lpthread -ldl -lz -lnativetask $(PLATFORM_LDFLAGS) 
AM_CFLAGS = $(PRFLAGS) $(JNI_INCLUDE) -I$(SNAPPY_PREFIX)/include -Ijavah -I$(top_srcdir)/src -Wall $(NATIVETASK_FLAGS)

COMMONSOURCES = src/HivePlatform.cc

COMMONHEADERS = src/HivePlatform.h

TESTSOURCES =

include_HEADERS = $(COMMONHEADERS)
lib_LTLIBRARIES = libnativetaskhive.la

libnativetaskhive_la_SOURCES = $(COMMONSOURCES)
libnativetaskhive_la_CPPFLAGS = -g -O2 -DNDEBUG $(MACRO) -fPIC $(AM_CFLAGS)
libnativetaskhive_la_LDFLAGS =  -version-info 0:1:0 $(AM_LDFLAGS)

