/**
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

#include "MahoutUtils.h"
#include "StringUtil.h"
#include "WritableUtils.h"
#include "lib/NativeObjectFactory.h"

namespace NativeTask {

int MahoutUtils::StringTupleComparator(const char * src, uint32_t srcLength, const char * dest,
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

int MahoutUtils::VarIntComparator(const char * src, uint32_t srcLength, const char * dest,
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

int MahoutUtils::VarLongComparator(const char * src, uint32_t srcLength, const char * dest,
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

int MahoutUtils::GramComparator(const char * src, uint32_t srcLength, const char * dest,
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
  return NativeObjectFactory::BytesComparator(src + srcVarIntSize, srcGramSize, dest + destVarIntSize, destGramSize);
}
;

int MahoutUtils::GramKeyComparator(const char * src, uint32_t srcLength, const char * dest,
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
  return NativeObjectFactory::BytesComparator(src, srcBytesLength, dest, destBytesLength);
}

int MahoutUtils::SplitPartitionedComparator(const char * src, uint32_t srcLength, const char * dest,
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

int MahoutUtils::EntityEntityComparator(const char * src, uint32_t srcLength, const char * dest,
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

}
