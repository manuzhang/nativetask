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

#include <stdlib.h>
#include <string.h>

#include "lib/NativeObjectFactory.h"
#include "lib/primitives.h"
#include "util/StringUtil.h"
#include "util/WritableUtils.h"
#include "PigPlatform.h"

using NativeTask::Config;
using NativeTask::IOException;
using NativeTask::NativeObject;
using NativeTask::NativeObjectFactory;
using NativeTask::ObjectCreatorFunc;
using NativeTask::StringUtil;
using NativeTask::WritableUtils;

Config & config = NativeObjectFactory::GetConfig();
bool * SortOrder;      // order for order by keys
int OrderLen = 1;
bool * SecSortOrder;   // order for secondary sort keys
int SecOrderLen = 1;
bool IsSecSort = false;  // whether in secondary sort
bool HasNullField = false; // any null field in tuple

bool tuplecmp(const char * lhs, const char * rhs) {
  return PigPlatform::compareInnerTuples(lhs, rhs) == -1;
}


void PigPlatform::stringToBooleans(string & conf, bool *& order, int & len) {
  len = conf.length();
  order = new bool[len];
  for (int i = 0; i < len; i++) {
    order[i] = conf.at(i) == '0' ? false : true;
  }
}

void PigPlatform::setSortOrder() {
  string conf = config.get(NATIVE_PIG_SORT_ORDER, "1");
  stringToBooleans(conf, SortOrder, OrderLen);
}

int8_t PigPlatform::ReadByte(const char * src) {
  return *(int8_t*) src;
}

uint8_t PigPlatform::ReadUnsignedByte(const char * src) {
  return *(uint8_t*) src;
}

int16_t PigPlatform::ReadShort(const char * src) {
  uint16_t ret = *(uint16_t*) src;
  return ((ret >> 8) | (ret << 8));
}

uint16_t PigPlatform::ReadUnsignedShort(const char * src) {
  uint16_t ret = *(uint16_t*) src;
  return ((ret >> 8) | (ret << 8));
}

int32_t PigPlatform::ReadInt(const char * src) {
  return (int32_t) bswap(*(uint32_t*) src);
}

int64_t PigPlatform::ReadLong(const char * src) {
  return (int64_t) bswap64(*(uint64_t*) src);
}

float PigPlatform::ReadFloat(const char * src) {
  uint32_t ret = bswap(*(uint32_t*) src);
  return *(float*) &ret;
}

double PigPlatform::ReadDouble(const char * src) {
  uint64_t ret = bswap64(*(uint64_t*) src);
  return *(double*) &ret;
}

void PigPlatform::ReadUTF(const char *& src, string * ret) {
  uint16_t len = ReadUnsignedShort(src);
  src += 2;
  const char * end = src + len;
  while (src != end) {
    const uint8_t byte1 = ReadByte(src);
    if ((byte1 & 0x80) == 0) {
      // byte1 -- | 0 |  bits 6-0 |
      ret->push_back((char) (byte1 & 0x7f));
    } else if ((byte1 & 0xe0) == 0xc0) {
      src++;
      const uint8_t byte2 = ReadByte(src);
      if ((byte2 & 0xc0) == 0x80) {
        // byte1 -- | 1 1 0 | bits 10-6 |
        // byte2 -- | 1 0 | bits 5-0    |
        char c = (byte1 & 0x1f) >> 2;
        if (0 != c) {
          ret->push_back(c);
        }
        ret->push_back((char) ((byte1 & 0x3) << 6 | (byte2 & 0x3f)));
      } else {
        THROW_EXCEPTION_EX(IOException, "Invalid 2-byte UTF byte2: %d", byte2);
      }
    } else if ((byte1 & 0xf0) == 0xe0) {
      src++;
      // byte1 -- | 1 1 1 0 | bits 15-12 |
      // byte2 -- | 1 0 | bits 11-6      |
      // byte3 -- | 1 0 | bits 5-0       |
      const uint8_t byte2 = ReadByte(src);
      if ((byte2 & 0xc0) == 0x80) {
        src++;
        const uint8_t byte3 = ReadByte(src);
        if ((byte3 & 0xc0) == 0x80) {
          char c = (byte1 & 0xf) << 4 | (byte2 & 0x3f) >> 2;
          if (0 != c) {
            ret->push_back(c);
          }
          ret->push_back((char) ((byte2 & 0x3) << 6 | (byte3 & 0x3f)));
        } else {
          THROW_EXCEPTION_EX(IOException, "Invalid 3-byte UTF byte3: %d",
              byte3);
        }
      } else {
        THROW_EXCEPTION_EX(IOException, "Invalid 3-byte UTF byte2: %d", byte2);
      }
    } else {
      THROW_EXCEPTION_EX(IOException, "Invalid UTF byte1: %d", byte1);
    }
    src++;
  }
}

void PigPlatform::ReadString(const char *& src, string * ret) {
  int len = ReadInt(src);
  src += 4;
  ret->append(src, len);
  src += len;
}

bool PigPlatform::ReadPigBool(const char type) {
  switch (type) {
  case PIG_BOOLEAN_FALSE:
    return false;
  case PIG_BOOLEAN_TRUE:
    return true;
  default:
    THROW_EXCEPTION_EX(IOException, "unknown Pig boolean type: %d", type);
  }
}

int PigPlatform::ReadPigInt(const char *& src, const char type) {
  int ret = 0;
  switch (type) {
  case PIG_INTEGER_0:
    return 0;
  case PIG_INTEGER_1:
    return 1;
  case PIG_INTEGER_INBYTE: {
    ret = ReadByte(src);
    src++;
    break;
  }
  case PIG_INTEGER_INSHORT: {
    ret = ReadShort(src);
    src += 2;
    break;
  }
  case PIG_INTEGER: {
    ret = ReadInt(src);
    src += 4;
    break;
  }
  default:
    THROW_EXCEPTION_EX(IOException, "unknown Pig integer type: %d", type);
  }
  return ret;
}

long PigPlatform::ReadPigLong(const char *& src, const char type) {
  long ret = 0;
  switch (type) {
  case PIG_LONG_0:
    return 0;
  case PIG_LONG_1:
    return 1;
  case PIG_LONG_INBYTE: {
    ret = ReadByte(src);
    src++;
    break;
  }
  case PIG_LONG_INSHORT: {
    ret = ReadShort(src);
    src += 2;
    break;
  }
  case PIG_LONG_ININT: {
    ret = ReadInt(src);
    src += 4;
    break;
  }
  case PIG_LONG: {
    ret = ReadLong(src);
    src += 8;
    break;
  }
  default:
    THROW_EXCEPTION_EX(IOException, "unknown Pig long type: %d", type);
  }
  return ret;
}

void PigPlatform::ReadPigCharArray(const char *& src, const char type,
    string * ret) {
  switch (type) {
  case PIG_SMALLCHARARRAY: {
    ReadUTF(src, ret);
    break;
  }

  case PIG_CHARARRAY: {
    ReadString(src, ret);
    break;
  }
  default:
    THROW_EXCEPTION_EX(IOException, "unknown Pig chararray type: %d", type);
  }
}

void PigPlatform::ReadPigMap(const char *& src, int size,
    map<string *, const char *> * m) {
  for (int i = 0; i < size; i++) {
    char keyType = src[0];
    src++;
    string * key = new string();
    ReadPigCharArray(src, keyType, key);
    const char * val = src;
    (*m)[key] = val;
    nextField(src);
  }
}

void PigPlatform::ReadPigBag(const char *& src, int size,
    list<const char *> * s) {
  for (int i = 0; i < size; i++) {
    const char * val = src;
    s->push_back(val);
    nextField(src);
  }
}

void PigPlatform::nextField(const char *& src) {
  char type = src[0];
  nextField(++src, type);
}

void PigPlatform::nextField(const char *& src, char type) {
  switch (type) {
  case PIG_BOOLEAN_TRUE:
  case PIG_BOOLEAN_FALSE:
  case PIG_INTEGER_0:
  case PIG_INTEGER_1:
  case PIG_LONG_0:
  case PIG_LONG_1:
  case PIG_NULL:
    break;

  case PIG_BYTE:
  case PIG_INTEGER_INBYTE:
  case PIG_LONG_INBYTE: {
    src++;
    break;
  }

  case PIG_INTEGER_INSHORT:
  case PIG_LONG_INSHORT: {
    src += 2;
    break;
  }
  case PIG_INTEGER:
  case PIG_LONG_ININT:
  case PIG_FLOAT: {
    src += 4;
    break;
  }
  case PIG_LONG:
  case PIG_DOUBLE: {
    src += 8;
    break;
  }
  case PIG_DATETIME: {
    src += 10;
    break;
  }

  case PIG_SMALLCHARARRAY: {
    int len = ReadShort(src);
    src += 2 + len;
    break;
  }
  case PIG_CHARARRAY: {
    int len = ReadInt(src);
    src += 4 + len;
    break;
  }

  case PIG_TINYBYTEARRAY:
  case PIG_SMALLBYTEARRAY:
  case PIG_BYTEARRAY: {
    int size = getDataSize(src, type);
    src += size;
    break;
  }

  case PIG_TUPLE_0:
  case PIG_TUPLE_1:
  case PIG_TUPLE_2:
  case PIG_TUPLE_3:
  case PIG_TUPLE_4:
  case PIG_TUPLE_5:
  case PIG_TUPLE_6:
  case PIG_TUPLE_7:
  case PIG_TUPLE_8:
  case PIG_TUPLE_9:
  case PIG_TUPLE:
  case PIG_TINYTUPLE:
  case PIG_SMALLTUPLE:
  case PIG_BAG:
  case PIG_SMALLBAG:
  case PIG_TINYBAG: {
    int size = getDataSize(src, type);
    for (int i = 0; i < size; i++) {
      nextField(src);
    }
    break;
  }
  case PIG_MAP:
  case PIG_TINYMAP:
  case PIG_SMALLMAP: {
    int size = getDataSize(src, type);
    for (int i = 0; i < size; i++) {
      nextField(src); // key
      nextField(src); // value
    }
    break;
  }
  case PIG_GENERIC:
    THROW_EXCEPTION(IOException, "do not support generic WritableComparable");
    break;
  case PIG_INTERNALMAP: {
    int len = ReadInt(src);
    src += len;
    for (int i = 0; i < len; i++) {
      nextField(src); // key
      nextField(src); // value
    }
    break;
  }

  default:
    THROW_EXCEPTION_EX(IOException, "Unexpected Sedes type: %d", type);
  }
}

int PigPlatform::PigNullableWritableComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength,
    ComparatorPtr comparator) {
  char srcNull = src[0];
  char destNull = dest[0];

  if (0 == srcNull && 0 == destNull) {
    return comparator(src + 1, srcLength - 2, dest + 1, destLength - 2);
  } else if (1 == srcNull && 1 == destNull) {
    return 0;
  } else if (1 == srcNull) {
    return -1;
  } else {
    return 1;
  }
}

/* length (zero-compressed encoding) + Text */
int PigPlatform::PigTextComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  uint32_t sllen, dllen;
  uint32_t sblen = WritableUtils::ReadVInt(src, sllen);
  uint32_t dblen = WritableUtils::ReadVInt(dest, dllen);
  return compareBytes(src + sllen, sblen, dest + dllen, dblen);
}

int PigPlatform::PigBytesComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  // src[0], dest[0] == PIG_TUPLE_1
  char srcType = src[1];
  char destType = dest[1];
  if (interSedesTypeToDataType(src[1]) != PigByteArrayType
      || interSedesTypeToDataType(dest[1]) != PigByteArrayType) {
    return compareTuples(src, dest);
  } else {
    src += 2;
    dest += 2;
    int srcSize = getDataSize(src, srcType);
    int destSize = getDataSize(dest, destType);
    return compareBytes(src, srcSize, dest, destSize);
  }
}

int PigPlatform::PigTupleComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  char srcNull = src[0];
  char destNull = dest[0];

  int c = 0;
  if (0 == srcNull && 0 == destNull) {
    c = compareTuples(++src, ++dest);
  } else {
    if (1 == srcNull && 1 == destNull) {
      c = 0;
    } else if (1 == srcNull) {
      c = -1;
    } else {

      c = 1;
    }
    if (1 == OrderLen && !SortOrder[0])
      c *= -1;
  }
  return c;
}

/*
 * copy inline function BytesComparator here
 */
int PigPlatform::compareBytes(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {

  uint32_t minlen = std::min(srcLength, destLength);
  int64_t ret = fmemcmp(src, dest, minlen);
  if (ret > 0) {
    return 1;
  } else if (ret < 0) {
    return -1;
  }
  return srcLength - destLength;
}

int PigPlatform::compareMaps(map<string *, const char *> & left,
    map<string *, const char *> & right) {
  int ls = left.size();
  int rs = right.size();
  if (ls < rs)
    return -1;
  else if (ls > rs)
    return 1;
  else {
    map<string *, const char *>::iterator lit = left.begin();
    map<string *, const char *>::iterator rit = right.begin();

    while (lit != left.end()) {
      int c = compare<string>(*(lit->first), *(rit->first));
      if (0 != c) {
        return c;
      } else {
        c = compareFields(lit->second, rit->second, NULL, 0);
        if (0 != c) {
          return c;
        }
      }
      lit++;
      rit++;
    }
    return 0;
  }
}

int PigPlatform::compareTuples(const char *& src, const char *& dest) {
  char srcType = src[0];
  char destType = dest[0];
  int srcSize = getDataSize(++src, srcType);
  int destSize = getDataSize(++dest, destType);
  if (srcSize < destSize) {
    return -1;
  } else if (srcSize > destSize) {
    return 1;
  } else {
    if (IsSecSort) {
      // compound tuple key (main_key, secondary_key)
      if (srcSize != 2)
        THROW_EXCEPTION(IOException,
            "Pig secondary sort, key number doesn't equal to 2");
      int c = compareFields(src, dest, SortOrder, OrderLen);
      if (0 == c)
        return compareFields(src, dest, SecSortOrder, SecOrderLen);
      else
        return c;
    } else {
      // one tuple key
      for (int i = 0; i < srcSize; i++) {
        int c = compareFields(src, dest, new bool[0], 0);
        if (c != 0) {
          if (OrderLen > 1 && i < OrderLen && !SortOrder[i])
            c *= -1;
          // if there is only one entry in the SortOrder
          // it means it's for the whole tuple
          else if (1 == OrderLen && !SortOrder[0])
            c *= -1;
          return c;
        }
      }
      return 0;
    }
  }
}

// inner tuples never have sort order
int PigPlatform::compareInnerTuples(const char *& src, const char *& dest) {
  char srcType = src[0];
  char destType = dest[0];
  return compareInnerTuples(++src, srcType, ++dest, destType);
}

int PigPlatform::compareInnerTuples(const char *& src, const char srcType,
    const char *& dest, const char destType) {
  int srcSize = getDataSize(src, srcType);
  int destSize = getDataSize(dest, destType);
  if (srcSize < destSize) {
    return -1;
  } else if (srcSize > destSize) {
    return 1;
  } else {
    for (int i = 0; i < srcSize; i++) {
      int c = compareFields(src, dest, NULL, 0);
      if (c != 0)
        return c;
    }
    return 0;
  }
}

int PigPlatform::compareBags(const char *& src, const char srcType,
    const char *& dest, const char destType) {
  int srcSize = getDataSize(src, srcType);
  int destSize = getDataSize(dest, destType);

  if (srcSize < destSize) {
    return -1;
  } else if (srcSize > destSize) {
    return 1;
  } else {
    list<const char *> * srcList = new list<const char *>();
    list<const char *> * destList = new list<const char *>();
    ReadPigBag(src, srcSize, srcList);
    ReadPigBag(dest, destSize, destList);

    srcList->sort(tuplecmp);
    destList->sort(tuplecmp);
    list<const char *>::iterator srcIt = srcList->begin();
    list<const char *>::iterator destIt = destList->begin();

    while (srcIt != srcList->end() && destIt != destList->end()) {
      const char * srcTuple = *srcIt;
      const char * destTuple = *destIt;
      int c = compareInnerTuples(srcTuple, destTuple);
      if (c != 0) {
        return c;
      }
      srcIt++;
      destIt++;
    }

    delete srcList;
    delete destList;
    return 0;
  }
}

int PigPlatform::compareFields(const char *& src, const char *& dest,
    bool * order, int oLen) {
  char srcType = src[0];
  char destType = dest[0];
  char srcDataType = interSedesTypeToDataType(srcType);
  char destDataType = interSedesTypeToDataType(destType);
  int c = compare<char>(srcDataType, destDataType);
  if (0 == c) {
    src++;
    dest++;
    switch (srcDataType) {
    case PigNullType:
      if (order != NULL)
        HasNullField = true;
      break;
    case PigBoolType: {
      c = compare<bool>(ReadPigBool(srcType), ReadPigBool(destType));
      break;
    }
    case PigByteType: {
      c = ReadByte(src) - ReadByte(dest);
      src++;
      dest++;
      break;
    }
    case PigIntType: {
      c = compare<int>(ReadPigInt(src, srcType), ReadPigInt(dest, destType));
      break;
    }
    case PigLongType: {
      c = compare<long>(ReadPigLong(src, srcType), ReadPigLong(dest, destType));
      break;
    }
    case PigFloatType: {
      c = compare<float>(ReadFloat(src), ReadFloat(dest));
      src += 4;
      dest += 4;
      break;
    }
    case PigDoubleType: {
      c = compare<double>(ReadDouble(src), ReadDouble(dest));
      src += 8;
      dest += 8;
      break;
    }
    case PigDateTimeType: {
      // we ignore following 2-byte timezone
      c = compare<long>(ReadLong(src), ReadLong(dest));
      src += 10;
      dest += 10;
      break;
    }
    case PigByteArrayType: {
      int sblen = getDataSize(src, srcType);
      int dblen = getDataSize(dest, destType);
      c = compareBytes(src, sblen, dest, dblen);
      src += sblen;
      dest += dblen;
      break;
    }
    case PigCharArrayType: {
      string * lhs = new string();
      string * rhs = new string();
      ReadPigCharArray(src, srcType, lhs);
      ReadPigCharArray(dest, destType, rhs);
      c = compare<string>(*lhs, *rhs);
      delete lhs;
      delete rhs;
      break;
    }
    case PigMapType: {
      int srcSize = getDataSize(src, srcType);
      int destSize = getDataSize(dest, destType);
      map<string *, const char *> * srcMap = new map<string *, const char *>();
      map<string *, const char *> * destMap = new map<string *, const char *>();
      ReadPigMap(src, srcSize, srcMap);
      ReadPigMap(dest, destSize, destMap);
      c = compareMaps(*srcMap, *destMap);
      delete srcMap;
      delete destMap;
      break;
    }
    case PigGenericType: {
      // should've checked this at Java side
      THROW_EXCEPTION(IOException, "do not support generic WritableComparable");
    }
    case PigInternalMapType: {
      c = -1;
      break;
    }
    case PigTupleType: {
      int srcSize = getDataSize(src, srcType);
      int destSize = getDataSize(dest, destType);
      c = compare<int>(srcSize, destSize);
      if (0 == c) {
        for (int i = 0; i < srcSize; i++) {
          c = compareFields(src, dest, NULL, 0);
          if (c != 0 && order != NULL && oLen > 1 && i < oLen && !order[i]) {
            c *= -1;
            break;
          }
        }
      } else {
        return c;
      }
      break;
    }
    case PigBagType: {
      c = compareBags(src, srcType, dest, destType);
      break;
    }
    default: {
      THROW_EXCEPTION(IOException, "unknown type in compare");
    }
    }
  }

  if (order != NULL && 1 == oLen && !order[0])
    c *= -1;
  return c;
}

string PigPlatform::getPackageName() {
  return "org.apache.pig.impl.io";
}

int PigPlatform::getDataSize(const char *& src, const char type) {
  int size = 0;
  switch (type) {
  case PIG_TUPLE_0:
    return 0;
  case PIG_TUPLE_1:
    return 1;
  case PIG_TUPLE_2:
    return 2;
  case PIG_TUPLE_3:
    return 3;
  case PIG_TUPLE_4:
    return 4;
  case PIG_TUPLE_5:
    return 5;
  case PIG_TUPLE_6:
    return 6;
  case PIG_TUPLE_7:
    return 7;
  case PIG_TUPLE_8:
    return 8;
  case PIG_TUPLE_9:
    return 9;
  case PIG_TINYTUPLE:
  case PIG_TINYBAG:
  case PIG_TINYMAP:
  case PIG_TINYBYTEARRAY: {
    size = ReadUnsignedByte(src);
    src++;
    break;
  }
  case PIG_SMALLTUPLE:
  case PIG_SMALLBAG:
  case PIG_SMALLMAP:
  case PIG_SMALLBYTEARRAY: {
    size = ReadUnsignedShort(src);
    src += 2;
    break;
  }
  case PIG_TUPLE:
  case PIG_MAP:
  case PIG_BYTEARRAY: {
    size = ReadInt(src);
    src += 4;
    break;
  }
  case PIG_BAG: {
    size = ReadLong(src);
    src += 8;
    break;
  }
  default:
    THROW_EXCEPTION_EX(IOException, "Unknown Sedes type : %d", type);
  }
  if (size < 0) {
    THROW_EXCEPTION_EX(IOException, "Invalid size %d for a tuple", size);
  }
  return size;
}

char PigPlatform::interSedesTypeToDataType(const char type) {
  switch (type) {
  case PIG_BOOLEAN_TRUE:
  case PIG_BOOLEAN_FALSE:
    return PigBoolType;
  case PIG_BYTE:
    return PigByteType;
  case PIG_INTEGER:
  case PIG_INTEGER_0:
  case PIG_INTEGER_1:
  case PIG_INTEGER_INSHORT:
  case PIG_INTEGER_INBYTE:
    return PigIntType;
  case PIG_LONG:
  case PIG_LONG_INBYTE:
  case PIG_LONG_INSHORT:
  case PIG_LONG_ININT:
  case PIG_LONG_0:
  case PIG_LONG_1:
    return PigLongType;
  case PIG_FLOAT:
    return PigFloatType;
  case PIG_DOUBLE:
    return PigDoubleType;
  case PIG_DATETIME:
    return PigDateTimeType;
  case PIG_BYTEARRAY:
  case PIG_SMALLBYTEARRAY:
  case PIG_TINYBYTEARRAY:
    return PigByteArrayType;
  case PIG_CHARARRAY:
  case PIG_SMALLCHARARRAY:
    return PigCharArrayType;
  case PIG_TINYMAP:
  case PIG_SMALLMAP:
  case PIG_MAP:
    return PigMapType;
  case PIG_TUPLE:
  case PIG_SMALLTUPLE:
  case PIG_TINYTUPLE:
  case PIG_TUPLE_0:
  case PIG_TUPLE_1:
  case PIG_TUPLE_2:
  case PIG_TUPLE_3:
  case PIG_TUPLE_4:
  case PIG_TUPLE_5:
  case PIG_TUPLE_6:
  case PIG_TUPLE_7:
  case PIG_TUPLE_8:
  case PIG_TUPLE_9:
    return PigTupleType;
  case PIG_BAG:
  case PIG_SMALLBAG:
  case PIG_TINYBAG:
    return PigBagType;
  case PIG_GENERIC:
    return PigGenericType;
  case PIG_INTERNALMAP:
    return PigInternalMapType;
  case PIG_NULL:
    return PigNullType;
  default:
    THROW_EXCEPTION_EX(IOException, "Unexpected Sedes type: %d", type);
  }
}

// Pig Comparators

/* Boolean is 1 byte */
int PigPlatform::PigNullableBooleanComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 3)
      || (destLength != 2 && destLength != 3)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableBooleanWritable comparator, while src/dest length is not 2 or 3");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::ByteComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

/* Int is 4 bytes */
int PigPlatform::PigNullableIntComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 6)
      || (destLength != 2 && destLength != 6)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableIntWritable comparator, while src/dest length is not 2 or 6");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::IntComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

/* Long is 8 bytes */
int PigPlatform::PigNullableLongComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 10)
      || (destLength != 2 && destLength != 10)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableLongWritable comparator, while src/dest length is not 2 or 10");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::LongComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

/* Float is 4 bytes */
int PigPlatform::PigNullableFloatComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 6)
      || (destLength != 2 && destLength != 6)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableFloatWritable comparator, while src/dest length is not 2 or 6");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::FloatComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

/* Double is 8 bytes */
int PigPlatform::PigNullableDoubleComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 10)
      || (destLength != 2 && destLength != 10)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableDoubleWritable comparator, while src/dest length is not 2 or 10");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::DoubleComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

/* 8 bytes time + 2 bytes timezone but timezone is ignored */
int PigPlatform::PigNullableDateTimeComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  if ((srcLength != 2 && srcLength != 12)
      || (destLength != 2 && destLength != 12)) {
    THROW_EXCEPTION(IOException,
        "Pig NullableDatetimeWritable comparator, while src/dest length is not 2 or 12");
  }
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &NativeObjectFactory::LongComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

int PigPlatform::PigNullableTextComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &PigPlatform::PigTextComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

int PigPlatform::PigNullableTupleComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  IsSecSort = false;
  setSortOrder();
  return PigPlatform::PigTupleComparator(src, srcLength, dest, destLength);
}

/*
 * NullableBytesWritable is internally a PIG_TUPLE_1
 */
int PigPlatform::PigNullableBytesComparator(const char * src,
    uint32_t srcLength, const char * dest, uint32_t destLength) {
  IsSecSort = false;
  setSortOrder();
  int c = PigPlatform::PigNullableWritableComparator(src, srcLength, dest,
      destLength, &PigPlatform::PigBytesComparator);
  if (c != 0 && !SortOrder[0])
    c *= -1;
  return c;
}

int PigPlatform::PigSecondaryKeyComparator(const char * src, uint32_t srcLength,
    const char * dest, uint32_t destLength) {
  IsSecSort = true;
  setSortOrder();
  string conf = config.get(NATIVE_PIG_SECONDARY_SORT_ORDER, "1");
  stringToBooleans(conf, SecSortOrder, SecOrderLen);

  const char mqFlag = 0x80;
  const char idxSpace = 0x7F;
  char srcNull = src[0];
  char srcIndex = src[srcLength - 1];

  char destNull = dest[0];
  char destIndex = dest[destLength - 1] & idxSpace;

  if ((srcIndex & mqFlag) != 0) {
    if ((srcIndex & idxSpace) < (destIndex & idxSpace))
      return -1;
    else if ((srcIndex & idxSpace) > (destIndex & idxSpace))
      return 1;
  }

  srcIndex &= idxSpace;
  destIndex &= idxSpace;

  if (0 == srcNull && 0 == destNull) {
    int result = compareTuples(++src, ++dest);
    if (0 == result && HasNullField) {
      return compare<char>(srcIndex, destIndex);
    }
    return result;
  } else if (1 == srcNull && 1 == destNull) {
    return compare<char>(srcIndex, destIndex);
  } else if (1 == srcNull) {
    return -1;
  } else {
    return 1;
  }
}

DEFINE_NATIVE_LIBRARY(PigPlatform) {
  REGISTER_FUNCTION(PigPlatform::PigNullableBooleanComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableBytesComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableDateTimeComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableDoubleComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableFloatComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableIntComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableLongComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableTextComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigNullableTupleComparator, PigPlatform);
  REGISTER_FUNCTION(PigPlatform::PigSecondaryKeyComparator, PigPlatform);
}
