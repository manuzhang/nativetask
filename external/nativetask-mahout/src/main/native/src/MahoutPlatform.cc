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


#include "string.h"

#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"
#include "lib/primitives.h"
#include "util/StringUtil.h"
#include "util/WritableUtils.h"


#include "MahoutPlatform.h"


using NativeTask::IOException;
using NativeTask::NativeObject;
using NativeTask::NativeObjectFactory;
using NativeTask::ObjectCreatorFunc;
using NativeTask::StringUtil;
using NativeTask::WritableUtils;

int MahoutPlatform::StringTupleComparator(const char * src, uint32_t srcLength, const char * dest,
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
    int ret = NativeObjectFactory::BytesComparator(src, srcStringSize, dest, destStringSize);
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

int MahoutPlatform::VarIntComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int32_t from = ReadSignedVarInt(src, srcLength);
  int32_t to = ReadSignedVarInt(dest, destLength);
  if (from > to) {
    return 1;
  }
  if (from < to) {
    return -1;
  }
  return 0;
}
;

int MahoutPlatform::VarLongComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  int64_t from = ReadSignedVarLong(src, srcLength);
  int64_t to = ReadSignedVarLong(dest, destLength);
  if (from > to) {
    return 1;
  }
  if (from < to) {
    return -1;
  }
  return 0;
}
;

int MahoutPlatform::GramComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarIntSize = 0;
  uint32_t destVarIntSize = 0;
  uint32_t srcGramSize = ReadUnsignedVarInt(src, srcVarIntSize);
  uint32_t destGramSize = ReadUnsignedVarInt(dest, destVarIntSize);
  if ((srcVarIntSize + srcGramSize > srcLength) || (destVarIntSize + destGramSize > destLength)) {
    THROW_EXCEPTION_EX(IOException,
        "gram comparator out of boundary, first gram size is %d, " "and src length is %d, second gram size is %d, and dest length is %d",
        srcVarIntSize + srcGramSize, srcLength, destVarIntSize + destGramSize, destLength);
  }
  return NativeObjectFactory::BytesComparator(src + srcVarIntSize, srcGramSize, dest + destVarIntSize, destGramSize);
}
;

int MahoutPlatform::GramKeyComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarIntSize = 0;
  uint32_t destVarIntSize = 0;
  uint32_t srcBytesLength = ReadUnsignedVarInt(src, srcVarIntSize);
  uint32_t destBytesLength = ReadUnsignedVarInt(dest, destVarIntSize);
  src += srcVarIntSize;
  dest += destVarIntSize;
  ReadUnsignedVarInt(src, srcVarIntSize);
  ReadUnsignedVarInt(dest, destVarIntSize);
  src += srcVarIntSize;
  dest += destVarIntSize;
  return NativeObjectFactory::BytesComparator(src, srcBytesLength, dest, destBytesLength);
}
;

int MahoutPlatform::SplitPartitionedComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarSize = 0;
  uint32_t destVarSize = 0;
  uint32_t srcTaskId = ReadUnsignedVarInt(src, srcVarSize);
  uint32_t destTaskId = ReadUnsignedVarInt(dest, destVarSize);
  src += srcVarSize;
  dest += destVarSize;
  uint64_t srcItemOrdinal = ReadUnsignedVarLong(src, srcVarSize);
  uint64_t destItemOrdinal = ReadUnsignedVarLong(dest, destVarSize);
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

int MahoutPlatform::EntityEntityComparator(const char * src, uint32_t srcLength, const char * dest,
    uint32_t destLength) {
  uint32_t srcVarSize = 0;
  uint32_t destVarSize = 0;
  int64_t srcField_a = ReadSignedVarLong(src, srcVarSize);
  int64_t destFiled_a = ReadSignedVarLong(dest, destVarSize);
  src += srcVarSize;
  dest += destVarSize;
  int64_t srcField_b = ReadSignedVarLong(src, srcVarSize);
  int64_t destFiled_b = ReadSignedVarLong(dest, destVarSize);
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

uint32_t MahoutPlatform::ReadUnsignedVarInt(const char * pos, uint32_t & len) {
  uint32_t value = 0;
  int i = 0;
  while (((*pos) & 0x80) != 0 && i <= 35) {
    value |= ((*pos) & 0x7F) << i;
    i += 7;
    pos++;
  }
  len = i / 7 + 1;
  return value | (*pos << i);
}

uint64_t MahoutPlatform::ReadUnsignedVarLong(const char * pos, uint32_t & len) {
  uint64_t value = 0;
  int i = 0;
  while (((*pos) & 0x80) != 0 && i <= 63) {
    value |= ((uint64_t)(*pos) & 0x7F) << i;
    i += 7;
    pos++;
  }
  len = i / 7 + 1;
  return value | ((uint64_t)(*pos) << i);
}

int32_t MahoutPlatform::ReadSignedVarInt(const char * pos, uint32_t & len) {
  int32_t raw = ReadUnsignedVarInt(pos, len);
  int32_t tmp = (((raw << 31) >> 31) ^ raw) >> 1;
  return tmp ^ (raw & (1 << 31));
}

int64_t MahoutPlatform::ReadSignedVarLong(const char * pos, uint32_t & len) {
  int64_t raw = ReadUnsignedVarLong(pos, len);
  int64_t tmp = (((raw << 63) >> 63) ^ raw) >> 1;
  return tmp ^ (raw & (1LL << 63));
}

void MahoutPlatform::WriteUnsignedVarInt(uint32_t num, char * pos, uint32_t & len) {
  len = 0;
  while ((num & 0xFFFFFF80) != 0L) {
    *pos = (num & 0x7F) | 0x80;
    num >>= 7;
    pos++;
    len++;
  }
  len++;
  *pos = num & 0x7F;
}

void MahoutPlatform::WriteUnsignedVarLong(uint64_t num, char * pos, uint32_t & len) {
  len = 0;
  while ((num & 0xFFFFFFFFFFFFFF80L) != 0L) {
    *pos = (num & 0x7F) | 0x80;
    num >>= 7;
    pos++;
    len++;
  }
  len++;
  *pos = num & 0x7F;
}

void MahoutPlatform::WriteSignedVarInt(int32_t num, char * pos, uint32_t & len) {
  WriteUnsignedVarInt((num << 1) ^ (num >> 31), pos, len);
}

void MahoutPlatform::WriteSignedVarLong(int64_t num, char * pos, uint32_t & len) {
  WriteUnsignedVarLong((num << 1) ^ (num >> 63), pos, len);
}

DEFINE_NATIVE_LIBRARY(MahoutPlatform) {
  REGISTER_FUNCTION(MahoutPlatform::StringTupleComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::VarIntComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::VarLongComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::GramComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::GramKeyComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::SplitPartitionedComparator, MahoutPlatform);
  REGISTER_FUNCTION(MahoutPlatform::EntityEntityComparator, MahoutPlatform);
}

