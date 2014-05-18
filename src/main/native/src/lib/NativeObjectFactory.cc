/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <signal.h>
#ifndef __CYGWIN__
#include <execinfo.h>
#endif
#include "commons.h"
#include "NativeTask.h"
#include "NativeObjectFactory.h"
#include "NativeLibrary.h"
#include "BufferStream.h"
#include "util/PigUtils.h"
#include "util/StringUtil.h"
#include "util/SyncUtils.h"
#include "util/WritableUtils.h"
#include "handler/BatchHandler.h"
#include "handler/MCollectorOutputHandler.h"
#include "handler/MMapperHandler.h"
#include "handler/MMapTaskHandler.h"
#include "handler/RReducerHandler.h"
#include "handler/CombineHandler.h"
#include "lib/LineRecordReader.h"
#include "lib/LineRecordWriter.h"
#include "lib/TotalOrderPartitioner.h"
#include "lib/TeraSort.h"
#include "lib/WordCount.h"

using namespace NativeTask;

// TODO: just for debug, should be removed
extern "C" void handler(int sig) {
  void *array[10];
  size_t size;

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);

#ifndef __CYGWIN__
  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  backtrace_symbols_fd(array, size, 2);
#endif

  exit(1);
}

DEFINE_NATIVE_LIBRARY(NativeTask) {
  //signal(SIGSEGV, handler);
  REGISTER_CLASS(BatchHandler, NativeTask);
  REGISTER_CLASS(CombineHandler, NativeTask);
  REGISTER_CLASS(MCollectorOutputHandler, NativeTask);
  REGISTER_CLASS(MMapperHandler, NativeTask);
  REGISTER_CLASS(MMapTaskHandler, NativeTask);
  REGISTER_CLASS(RReducerHandler, NativeTask);
  REGISTER_CLASS(Mapper, NativeTask);
  REGISTER_CLASS(Reducer, NativeTask);
  REGISTER_CLASS(Partitioner, NativeTask);
  REGISTER_CLASS(Folder, NativeTask);
  REGISTER_CLASS(LineRecordReader, NativeTask);
  REGISTER_CLASS(KeyValueLineRecordReader, NativeTask);
  REGISTER_CLASS(LineRecordWriter, NativeTask);
  REGISTER_CLASS(TotalOrderPartitioner, NativeTask);
  REGISTER_CLASS(TeraRecordReader, NativeTask);
  REGISTER_CLASS(TeraRecordWriter, NativeTask);
  REGISTER_CLASS(WordCountMapper, NativeTask);
  REGISTER_CLASS(IntSumReducer, NativeTask);
  REGISTER_CLASS(IntSumMapper, NativeTask);
  REGISTER_CLASS(TextIntRecordWriter, NativeTask);
  NativeObjectFactory::SetDefaultClass(BatchHandlerType, "NativeTask.BatchHandler");
  NativeObjectFactory::SetDefaultClass(MapperType, "NativeTask.Mapper");
  NativeObjectFactory::SetDefaultClass(ReducerType, "NativeTask.Reducer");
  NativeObjectFactory::SetDefaultClass(PartitionerType, "NativeTask.Partitioner");
  NativeObjectFactory::SetDefaultClass(FolderType, "NativeTask.Folder");
}

namespace NativeTask {

static Config G_CONFIG;

vector<NativeLibrary *> NativeObjectFactory::Libraries;
map<NativeObjectType, string> NativeObjectFactory::DefaultClasses;
Config * NativeObjectFactory::GlobalConfig = &G_CONFIG;
float NativeObjectFactory::LastProgress = 0;
Progress * NativeObjectFactory::TaskProgress = NULL;
string NativeObjectFactory::LastStatus;
set<Counter *> NativeObjectFactory::CounterSet;
vector<Counter *> NativeObjectFactory::Counters;
vector<uint64_t> NativeObjectFactory::CounterLastUpdateValues;
bool NativeObjectFactory::Inited = false;

static Lock FactoryLock;

bool NativeObjectFactory::Init() {
  ScopeLock<Lock> autolocak(FactoryLock);
  if (Inited == false) {
    // setup log device
    string device = GetConfig().get(NATIVE_LOG_DEVICE, "stderr");
    if (device == "stdout") {
      LOG_DEVICE = stdout;
    } else if (device == "stderr") {
      LOG_DEVICE = stderr;
    } else {
      LOG_DEVICE = fopen(device.c_str(), "w");
    }
    NativeTaskInit();
    NativeLibrary * library = new NativeLibrary("libnativetask.so", "NativeTask");
    library->_getObjectCreatorFunc = NativeTaskGetObjectCreator;
    Libraries.push_back(library);
    Inited = true;
    // load extra user provided libraries
    string libraryConf = GetConfig().get(NATIVE_CLASS_LIBRARY_BUILDIN, "");
    if (libraryConf.length() > 0) {
      vector<string> libraries;
      vector<string> pair;
      StringUtil::Split(libraryConf, ",", libraries, true);
      for (size_t i = 0; i < libraries.size(); i++) {
        pair.clear();
        StringUtil::Split(libraries[i], "=", pair, true);
        if (pair.size() == 2) {
          string & name = pair[0];
          string & path = pair[1];
          LOG("[NativeObjectLibrary] Try to load library [%s] with file [%s]", name.c_str(),
              path.c_str());
          if (false == RegisterLibrary(path, name)) {
            LOG("[NativeObjectLibrary] RegisterLibrary failed: name=%s path=%s", name.c_str(),
                path.c_str());
            return false;
          } else {
            LOG("[NativeObjectLibrary] RegisterLibrary success: name=%s path=%s", name.c_str(),
                path.c_str());
          }
        } else {
          LOG("[NativeObjectLibrary] Illegal native.class.libray: [%s] in [%s]",
              libraries[i].c_str(), libraryConf.c_str());
        }
      }
    }
    const char * version = GetConfig().get(NATIVE_HADOOP_VERSION);
    LOG("[NativeObjectLibrary] NativeTask library initialized with hadoop %s",
        version==NULL?"unkown":version);
  }
  return true;
}

void NativeObjectFactory::Release() {
  ScopeLock<Lock> autolocak(FactoryLock);
  for (ssize_t i = Libraries.size() - 1; i >= 0; i--) {
    delete Libraries[i];
    Libraries[i] = NULL;
  }
  Libraries.clear();
  for (size_t i = 0; i < Counters.size(); i++) {
    delete Counters[i];
  }
  Counters.clear();
  if (LOG_DEVICE != stdout && LOG_DEVICE != stderr) {
    fclose(LOG_DEVICE);
    LOG_DEVICE = stderr;
  }
  Inited = false;
}

void NativeObjectFactory::CheckInit() {
  if (Inited == false) {
    if (!Init()) {
      throw new IOException("Init NativeTask library failed.");
    }
  }
}

Config & NativeObjectFactory::GetConfig() {
  return *GlobalConfig;
}

Config * NativeObjectFactory::GetConfigPtr() {
  return GlobalConfig;
}

void NativeObjectFactory::SetTaskProgressSource(Progress * progress) {
  TaskProgress = progress;
}

float NativeObjectFactory::GetTaskProgress() {
  if (TaskProgress != NULL) {
    LastProgress = TaskProgress->getProgress();
  }
  return LastProgress;
}

void NativeObjectFactory::SetTaskStatus(const string & status) {
  LastStatus = status;
}

static Lock CountersLock;

void NativeObjectFactory::GetTaskStatusUpdate(string & statusData) {
  // Encoding:
  // progress:float
  // status:Text
  // Counter number
  // Counters[group:Text, name:Text, incrCount:Long]
  OutputStringStream os(statusData);
  float progress = GetTaskProgress();
  WritableUtils::WriteFloat(&os, progress);
  WritableUtils::WriteText(&os, LastStatus);
  LastStatus.clear();
  {
    ScopeLock<Lock> AutoLock(CountersLock);
    uint32_t numCounter = (uint32_t)Counters.size();
    WritableUtils::WriteInt(&os, numCounter);
    for (size_t i = 0; i < numCounter; i++) {
      Counter * counter = Counters[i];
      uint64_t newCount = counter->get();
      uint64_t incr = newCount - CounterLastUpdateValues[i];
      CounterLastUpdateValues[i] = newCount;
      WritableUtils::WriteText(&os, counter->group());
      WritableUtils::WriteText(&os, counter->name());
      WritableUtils::WriteLong(&os, incr);
    }
  }
}

Counter * NativeObjectFactory::GetCounter(const string & group, const string & name) {
  ScopeLock<Lock> AutoLock(CountersLock);
  Counter tmpCounter(group, name);
  set<Counter *>::iterator itr = CounterSet.find(&tmpCounter);
  if (itr != CounterSet.end()) {
    return *itr;
  }
  Counter * ret = new Counter(group, name);
  Counters.push_back(ret);
  CounterLastUpdateValues.push_back(0);
  CounterSet.insert(ret);
  return ret;
}

void NativeObjectFactory::RegisterClass(const string & clz, ObjectCreatorFunc func) {
  NativeTaskClassMap__[clz] = func;
}

NativeObject * NativeObjectFactory::CreateObject(const string & clz) {
  ObjectCreatorFunc creator = GetObjectCreator(clz);
  return creator ? creator() : NULL;
}

void * NativeObjectFactory::GetFunction(const string & funcName) {
  CheckInit();
  {
    for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
        ritr != Libraries.rend(); ritr++) {
      void * ret = (*ritr)->getFunction(funcName);
      if (NULL != ret) {
        return ret;
      }
    }
    return NULL;
  }
}

ObjectCreatorFunc NativeObjectFactory::GetObjectCreator(const string & clz) {
  CheckInit();
  {
    for (vector<NativeLibrary*>::reverse_iterator ritr = Libraries.rbegin();
        ritr != Libraries.rend(); ritr++) {
      ObjectCreatorFunc ret = (*ritr)->getObjectCreator(clz);
      if (NULL != ret) {
        return ret;
      }
    }
    return NULL;
  }
}

void NativeObjectFactory::ReleaseObject(NativeObject * obj) {
  delete obj;
}

bool NativeObjectFactory::RegisterLibrary(const string & path, const string & name) {
  CheckInit();
  {
    NativeLibrary * library = new NativeLibrary(path, name);
    bool ret = library->init();
    if (!ret) {
      delete library;
      return false;
    }
    Libraries.push_back(library);
    return true;
  }
}

static Lock DefaultClassesLock;

void NativeObjectFactory::SetDefaultClass(NativeObjectType type, const string & clz) {
  ScopeLock<Lock> autolocak(DefaultClassesLock);
  DefaultClasses[type] = clz;
}

NativeObject * NativeObjectFactory::CreateDefaultObject(NativeObjectType type) {
  CheckInit();
  {
    if (DefaultClasses.find(type) != DefaultClasses.end()) {
      string clz = DefaultClasses[type];
      return CreateObject(clz);
    }
    LOG("[NativeObjectLibrary] Default class for NativeObjectType %s not found",
        NativeObjectTypeToString(type).c_str());
    return NULL;
  }
}

inline int BytesComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {

  uint32_t minlen = std::min(srcLength, destLength);
  int64_t ret = fmemcmp(src, dest, minlen);
  if (ret > 0) {
    return 1;
  } else if (ret < 0) {
    return -1;
  }
  return srcLength - destLength;
}
;

inline int ByteComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  return (*src) - (*dest);
}
;

inline int IntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int result = (*src) - (*dest);
  if (result == 0) {
    uint32_t from = bswap(*(uint32_t*)src);
    uint32_t to = bswap(*(uint32_t*)dest);
    if (from > to) {
      return 1;
    } else if (from == to) {
      return 0;
    } else {
      return -1;
    }
  }
  return result;
}
;

inline int LongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int result = (int)(*src) - (int)(*dest);
  if (result == 0) {

    uint64_t from = bswap64(*(uint64_t*)src);
    uint64_t to = bswap64(*(uint64_t*)dest);
    if (from > to) {
      return 1;
    } else if (from == to) {
      return 0;
    } else {
      return -1;
    }
  }
  return result;
}
;

inline int VIntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int32_t from = WritableUtils::ReadVInt(src, srcLength);
  int32_t to = WritableUtils::ReadVInt(dest, destLength);
  if (from > to) {
    return 1;
  } else if (from == to) {
    return 0;
  } else {
    return -1;
  }
}
;

inline int VLongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int64_t from = WritableUtils::ReadVLong(src, srcLength);
  int64_t to = WritableUtils::ReadVLong(dest, destLength);
  if (from > to) {
    return 1;
  } else if (from == to) {
    return 0;
  } else {
    return -1;
  }
}
;

inline int FloatComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if (srcLength != 4 || destLength != 4) {
    THROW_EXCEPTION_EX(IOException, "float comparator, while src/dest lengt is not 4");
  }

  uint32_t from = bswap(*(uint32_t*)src);
  uint32_t to = bswap(*(uint32_t*)dest);

  float * srcValue = (float *)(&from);
  float * destValue = (float *)(&to);

  if ((*srcValue) < (*destValue)) {
    return -1;
  } else if ((*srcValue) == (*destValue)) {
    return 0;
  } else {
    return 1;
  }
}
;

inline int DoubleComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if (srcLength != 8 || destLength != 8) {
    THROW_EXCEPTION_EX(IOException, "double comparator, while src/dest lengt is not 4");
  }

  uint64_t from = bswap64(*(uint64_t*)src);
  uint64_t to = bswap64(*(uint64_t*)dest);

  double * srcValue = (double *)(&from);
  double * destValue = (double *)(&to);
  if ((*srcValue) < (*destValue)) {
    return -1;
  } else if ((*srcValue) == (*destValue)) {
    return 0;
  } else {
    return 1;
  }
}
;

inline int StringTupleComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcSize = bswap(*(uint32_t*)src);
  uint32_t destSize = bswap(*(uint32_t*)dest);
  uint32_t minsize = std::min(srcSize, destSize);
  src += sizeof(uint32_t);
  dest += sizeof(uint32_t);
  uint32_t srcStringSize;
  uint32_t srcIntLength;
  uint32_t destStringSize;
  uint32_t destIntLength;
  for (int i = 0; i < minsize; i++) {
    srcStringSize = WritableUtils::ReadVInt(src, srcIntLength);
    destStringSize = WritableUtils::ReadVInt(dest, destIntLength);
    src += srcIntLength;
    dest += destIntLength;
    int ret = BytesComparator(src, srcStringSize, dest, destStringSize);
    if (ret != 0) {
      return ret;
    }
    src += srcStringSize;
    dest += destStringSize;
  }
  if (srcSize < destSize) {
    return -1;
  } else if (srcSize > destSize) {
    return 1;
  }
  return 0;
}
;

inline int VarIntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int32_t from = WritableUtils::ReadSignedVarInt(src, srcLength);
  int32_t to = WritableUtils::ReadSignedVarInt(dest, destLength);
  if (from > to) {
    return 1;
  }
  if (from < to) {
    return -1;
  }
  return 0;
}
;

inline int VarLongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int64_t from = WritableUtils::ReadSignedVarLong(src, srcLength);
  int64_t to = WritableUtils::ReadSignedVarLong(dest, destLength);
  if (from > to) {
    return 1;
  }
  if (from < to) {
    return -1;
  }
  return 0;
}
;

inline int GramComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarIntSize = 0;
  uint32_t destVarIntSize = 0;
  uint32_t srcGramSize = WritableUtils::ReadUnsignedVarInt(src, srcVarIntSize);
  uint32_t destGramSize = WritableUtils::ReadUnsignedVarInt(dest, destVarIntSize);
  if ((srcVarIntSize + srcGramSize > srcLength) || (destVarIntSize + destGramSize > destLength)) {
    THROW_EXCEPTION_EX(IOException,
        "gram comparator out of boundary, first gram size is %d, " "and src length is %d, second gram size is %d, and dest length is %d",
        srcVarIntSize + srcGramSize, srcLength, destVarIntSize + destGramSize, destLength);
  }
  return BytesComparator(src + srcVarIntSize, srcGramSize, dest + destVarIntSize, destGramSize);
}
;

inline int GramKeyComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarIntSize = 0;
  uint32_t destVarIntSize = 0;
  uint32_t srcBytesLength = WritableUtils::ReadUnsignedVarInt(src, srcVarIntSize);
  uint32_t destBytesLength = WritableUtils::ReadUnsignedVarInt(dest, destVarIntSize);
  src += srcVarIntSize;
  dest += destVarIntSize;
  WritableUtils::ReadUnsignedVarInt(src, srcVarIntSize);
  WritableUtils::ReadUnsignedVarInt(dest, destVarIntSize);
  src += srcVarIntSize;
  dest += destVarIntSize;
  return BytesComparator(src, srcBytesLength, dest, destBytesLength);
}
;

inline int SplitPartitionedComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarSize = 0;
  uint32_t destVarSize = 0;
  uint32_t srcTaskId = WritableUtils::ReadUnsignedVarInt(src, srcVarSize);
  uint32_t destTaskId = WritableUtils::ReadUnsignedVarInt(dest, destVarSize);
  src += srcVarSize;
  dest += destVarSize;
  uint64_t srcItemOrdinal = WritableUtils::ReadUnsignedVarLong(src, srcVarSize);
  uint64_t destItemOrdinal = WritableUtils::ReadUnsignedVarLong(dest, destVarSize);
  if (srcTaskId < destTaskId) {
    return -1;
  }
  if (srcTaskId > destTaskId) {
    return 1;
  }
  if (srcItemOrdinal < destItemOrdinal) {
    return -1;
  }
  if (srcItemOrdinal > destItemOrdinal) {
    return 1;
  }
  return 0;
}
;

inline int EntityEntityComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarSize = 0;
  uint32_t destVarSize = 0;
  int64_t srcField_a = WritableUtils::ReadSignedVarLong(src, srcVarSize);
  int64_t destFiled_a = WritableUtils::ReadSignedVarLong(dest, destVarSize);
  src += srcVarSize;
  dest += destVarSize;
  int64_t srcField_b = WritableUtils::ReadSignedVarLong(src, srcVarSize);
  int64_t destFiled_b = WritableUtils::ReadSignedVarLong(dest, destVarSize);
  if (srcField_a < destFiled_a) {
    return -1;
  }
  if (srcField_a > destFiled_a) {
    return 1;
  }
  if (srcField_b < destFiled_b) {
    return -1;
  }
  if (srcField_b > destFiled_b) {
    return 1;
  }
  return 0;
}
;

// Pig Comparators

/* Boolean is 1 byte */
inline int PigNullableBooleanComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 3) || (destLength != 2 && destLength != 3)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableBooleanWritable comparator, while src/dest length is not 2 or 3");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &ByteComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

/* Int is 4 bytes */
inline int PigNullableIntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 6) || (destLength != 2 && destLength != 6)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableIntWritable comparator, while src/dest length is not 2 or 6");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &IntComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

/* Long is 8 bytes */
inline int PigNullableLongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 10) || (destLength != 2 && destLength != 10)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableLongWritable comparator, while src/dest length is not 2 or 10");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &LongComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

/* Float is 4 bytes */
inline int PigNullableFloatComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 6) || (destLength != 2 && destLength != 6)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableFloatWritable comparator, while src/dest length is not 2 or 6");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &FloatComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

/* Double is 8 bytes */
inline int PigNullableDoubleComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 10) || (destLength != 2 && destLength != 10)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableDoubleWritable comparator, while src/dest length is not 2 or 10");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &DoubleComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

/* 8 bytes time + 2 bytes timezone but timezone is ignored */
inline int PigNullableDateTimeComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 12) || (destLength != 2 && destLength != 12)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableDatetimeWritable comparator, while src/dest length is not 2 or 12");
  }
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength, &LongComparator);
  if (c != 0 && !PigUtils::getSortOrder())
    c *= -1;
  return c;
}

inline int PigNullableTextComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength,
      &PigUtils::PigTextComparator);
  if (c != 0 && !PigUtils::getSortOrder()) c *= -1;
  return c;
}

inline int PigNullableTupleComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  return PigUtils::PigTupleComparator(src, srcLength, dest, destLength);
}

/*
 * NullableBytesWritable is internally a PIG_TUPLE_1
 */
inline int PigNullableBytesComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int c = PigUtils::PigNullableWritableComparator(src, srcLength, dest, destLength,
      &PigUtils::PigBytesComparator);
  if (c != 0 && !PigUtils::getSortOrder()) c *= -1;
  return c;
}


inline ComparatorPtr getPigComparator(const PigWritableType keyType) {
  if (PigBooleanWritable == keyType) {
    return &PigNullableBooleanComparator;
  } else if (PigBytesWritable == keyType) {
    return &PigNullableBytesComparator;
  } else if (PigDateTimeWritable == keyType) {
    return &PigNullableDateTimeComparator;
  } else if (PigDoubleWritable == keyType) {
    return &PigNullableDoubleComparator;
  } else if (PigFloatWritable == keyType) {
    return &PigNullableFloatComparator;
  } else if (PigIntWritable == keyType) {
    return &PigNullableIntComparator;
  } else if (PigLongWritable == keyType) {
    return &PigNullableLongComparator;
  } else if (PigText == keyType) {
    return &PigNullableTextComparator;
  } else if (PigTuple == keyType) {
    return &PigNullableTupleComparator;
  } else {
    return NULL;
  }
}

// Pig Comparators

ComparatorPtr get_comparator(const KeyValueType keyType, const char * comparatorName) {
  if (NULL == comparatorName) {
    if (keyType == BytesType || keyType == TextType) {
      return &BytesComparator;
    } else if (keyType == ByteType || keyType == BoolType) {
      return &ByteComparator;
    } else if (keyType == IntType) {
      return &IntComparator;
    } else if (keyType == LongType) {
      return &LongComparator;
    } else if (keyType == FloatType) {
      return &FloatComparator;
    } else if (keyType == DoubleType) {
      return &DoubleComparator;
    } else if (keyType == VIntType) {
      return &VIntComparator;
    } else if (keyType == VLongType) {
      return &VLongComparator;
    } else if (keyType == StringTupleType) {
      return &StringTupleComparator;
    } else if (keyType == VarIntType) {
      return &VarIntComparator;
    } else if (keyType == VarLongType) {
      return &VarLongComparator;
    } else if (keyType == GramType) {
      return &GramComparator;
    } else if (keyType == GramKeyType) {
      return &GramKeyComparator;
    } else if (keyType == SplitPartitionedType) {
      return &SplitPartitionedComparator;
    } else if (keyType == EntityEntityType) {
      return &EntityEntityComparator;
    } else if (keyType == PigType) {
      if (PigUtils::isGroupOnly()) {
        LOG("[NativeObjectLibrary] key group only, using BytesComparator");
        return &BytesComparator;
      } else if (PigUtils::useSecondaryKey()){
        LOG("[NativeObjectLibrary] secondary key, using PigSecondaryKeyComparator");
        PigUtils::setSecSort(true);
        PigUtils::setSortOrder();
        PigUtils::setSecondarySortOrder();
        return &PigUtils::PigSecondaryKeyComparator;
      } else {
        LOG("[NativeObjectLibrary] order by key, using PigRawComparator");
        PigUtils::setSecSort(false);
        PigUtils::setSortOrder();
        return getPigComparator(PigUtils::getPigWritableType());
      }
    }
  } else {
    void * func = NativeObjectFactory::GetFunction(string(comparatorName));
    return (ComparatorPtr)func;
  }
  return NULL;
}

} // namespace NativeTask

