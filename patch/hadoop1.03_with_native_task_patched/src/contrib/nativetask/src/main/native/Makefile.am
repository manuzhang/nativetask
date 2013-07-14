export PLATFORM = `uname | tr [A-Z] [a-z]`
export BITS = `./getosbits.sh`

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

AM_LDFLAGS = -no-undefined  -L$(SNAPPY_PREFIX)/lib -lsnappy -lpthread -ldl -lz $(PLATFORM_LDFLAGS)
AM_CFLAGS = $(PRFLAGS) $(JNI_INCLUDE) -I$(SNAPPY_PREFIX)/include -Ijavah -I$(top_srcdir)/src -I$(top_srcdir)/src/util -I$(top_srcdir)/src/lib 

COMMONSOURCES = \
lz4/lz4.c                              \
cityhash/city.cc                       \
src/codec/BlockCodec.cc \
src/codec/GzipCodec.cc \
src/codec/Lz4Codec.cc \
src/codec/SnappyCodec.cc \
src/handler/BatchHandler.cc \
src/handler/EchoBatchHandler.cc \
src/handler/MCollectorOutputHandler.cc \
src/handler/MMapperHandler.cc \
src/handler/MMapTaskHandler.cc \
src/handler/RReducerHandler.cc \
src/handler/CombineHandler.cc \
src/lib/Buffers.cc \
src/lib/BufferStream.cc \
src/lib/Compressions.cc \
src/lib/FileSplit.cc \
src/lib/FileSystem.cc \
src/lib/IFile.cc \
src/lib/jniutils.cc \
src/lib/KVFile.cc \
src/lib/LineRecordReader.cc \
src/lib/LineRecordWriter.cc \
src/lib/Log.cc \
src/lib/MapOutputCollector.cc \
src/lib/MapOutputSpec.cc \
src/lib/mempool.cc \
src/lib/Merge.cc \
src/lib/NativeLibrary.cc \
src/lib/NativeObjectFactory.cc \
src/lib/NativeRuntimeJniImpl.cc \
src/lib/NativeTask.cc \
src/lib/SpillInfo.cc \
src/lib/Path.cc \
src/lib/Streams.cc \
src/lib/combiner.cc \
src/lib/TaskCounters.cc \
src/lib/TeraSort.cc \
src/lib/TotalOrderPartitioner.cc \
src/lib/WordCount.cc \
src/util/Checksum.cc \
src/util/Hash.cc \
src/util/Process.cc \
src/util/Random.cc \
src/util/StringUtil.cc \
src/util/SyncUtils.cc \
src/util/Timer.cc \
src/util/WritableUtils.cc               


COMMONHEADERS = \
src/util/Checksum.h                   \
src/util/Process.h                    \
src/util/StringUtil.h                 \
src/util/SyncUtils.h                  \
src/util/WritableUtils.h              \
src/lib/LineRecordReader.h            \
src/lib/LineRecordWriter.h            \
src/lib/commons.h                         \
src/lib/Log.h                             \
src/lib/primitives.h                      \
src/lib/Buffers.h                         \
src/lib/BufferStream.h                    \
src/lib/Compressions.h                    \
src/lib/FileSystem.h                      \
src/lib/Streams.h                         \
src/lib/NativeObjectFactory.h             \
src/NativeTask.h                      


TESTSOURCES = \
test/handler/TestRReducerHandler.cc   \
test/handler/TestMMapTaskHandler.cc   \
test/lib/TestLineRecordReader.cc      \
test/lib/TestTotalOrderPartitioner.cc \
test/lib/TestCombiner.cc \
test/util/TestChecksum.cc             \
test/util/TestHash.cc                 \
test/util/TestStringUtil.cc           \
test/util/TestSyncUtils.cc            \
test/util/TestWritableUtils.cc        \
test/util/TestProcess.cc              \
test/TestBuffers.cc                   \
test/TestCompressions.cc              \
test/TestException.cc                 \
test/TestFileSystem.cc                \
test/TestIFile.cc                     \
test/TestPrimitives.cc                \
test/TestSort.cc                      \
test/TestStream.cc                    \
test/TestMain.cc                      \
test/test_commons.cc                  

include_HEADERS = $(COMMONHEADERS)
lib_LTLIBRARIES = libnativetask.la libstreaming.la
noinst_PROGRAMS = nttest
noinst_SCRIPTS = test.sh

libnativetask_la_SOURCES = $(COMMONSOURCES)
libnativetask_la_CPPFLAGS = -g -O2 -DNDEBUG $(MACRO) -fPIC $(AM_CFLAGS)
libnativetask_la_LDFLAGS =  -version-info 0:1:0 $(AM_LDFLAGS)

libstreaming_la_SOURCES = examples/Streaming.cc
libstreaming_la_CPPFLAGS = -g -O2 -DNDEBUG $(MACRO) -fPIC $(AM_CFLAGS)
libstreaming_la_LDFLAGS = -version-info 0:1:0 $(AM_LDFLAGS)

nttest_SOURCES = $(TESTSOURCES) gtest/gtest-all.cc
nttest_LDADD = libnativetask.la
nttest_LDFLAGS = $(AM_LDFLAGS)
nttest_CPPFLAGS = -g -O2  $(MACRO) $(AM_CFLAGS) -I$(top_srcdir) -I$(top_srcdir)/test
