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

#ifndef PIGUTILS_H_
#define PIGUTILS_H_

#include "commons.h"

namespace NativeTask {

using std::map;
using std::list;
using std::string;

enum PigWritableType {
  PigBag = 1,
  PigBooleanWritable = 4,
  PigBytesWritable = 5,
  PigDateTimeWritable = 6,
  PigDoubleWritable = 7,
  PigFloatWritable = 8,
  PigIntWritable = 9,
  PigLongWritable = 10,
  PigText = 12,
  PigTuple = 13,
  PigError = -1
};

enum PigDataType {
  PigUnknownType = 0,
  PigNullType = 1,
  PigBoolType = 5,
  PigByteType = 6,
  PigIntType = 10,
  PigLongType = 15,
  PigFloatType = 20,
  PigDoubleType = 25,
  PigDateTimeType = 30,
  PigByteArrayType = 50,
  PigCharArrayType = 55,
  PigBigCharArrayType = 60,
  PigMapType = 100,
  PigTupleType = 110,
  PigBagType = 120,
  PigGenericType = 123,  // GenericWritableComparableType
  PigInternalMapType = 127,
  PigErrorType = -1
};

enum PigInterSedesType {
  PIG_BOOLEAN_TRUE = 0,
  PIG_BOOLEAN_FALSE = 1,

  PIG_BYTE = 2,

  PIG_INTEGER = 3,
  PIG_INTEGER_0 = 4,
  PIG_INTEGER_1 = 5,
  PIG_INTEGER_INSHORT = 6,
  PIG_INTEGER_INBYTE = 7,

  PIG_LONG = 8,
  PIG_FLOAT = 9,
  PIG_DOUBLE = 10,

  PIG_BYTEARRAY = 11,
  PIG_SMALLBYTEARRAY = 12,
  PIG_TINYBYTEARRAY = 13,

  PIG_CHARARRAY = 14,
  PIG_SMALLCHARARRAY = 15,

  PIG_MAP = 16,
  PIG_SMALLMAP = 17,
  PIG_TINYMAP = 18,

  PIG_TUPLE = 19,
  PIG_SMALLTUPLE = 20,
  PIG_TINYTUPLE = 21,

  PIG_BAG = 22,
  PIG_SMALLBAG = 23,
  PIG_TINYBAG = 24,

  PIG_GENERIC = 25,  // GENERIC_WRITABLECOMPARABLE
  PIG_INTERNALMAP = 26,

  PIG_NULL = 27,

  PIG_LONG_INBYTE = 31,
  PIG_LONG_INSHORT = 32,
  PIG_LONG_ININT = 33,
  PIG_LONG_0 = 34,
  PIG_LONG_1 = 35,

  PIG_TUPLE_0 = 36,
  PIG_TUPLE_1 = 37,
  PIG_TUPLE_2 = 38,
  PIG_TUPLE_3 = 39,
  PIG_TUPLE_4 = 40,
  PIG_TUPLE_5 = 41,
  PIG_TUPLE_6 = 42,
  PIG_TUPLE_7 = 43,
  PIG_TUPLE_8 = 44,
  PIG_TUPLE_9 = 45,

  PIG_DATETIME = 48
};

typedef bool (*CmpFunc)(const char *, const char *);

class PigUtils {

protected:
  static bool ReadPigBool(const char type);
  static int ReadPigInt(const char *& src, const char type);
  static long ReadPigLong(const char *& src, const char type);
  static void ReadPigMap(const char *& src, int size, map<string *, const char *> * m);
  static void ReadPigBag(const char *& src, int size, list<const char *> * s);
  static void ReadPigCharArray(const char *& src, const char type, string * ret);
  static void nextField(const char *& src, char byte);
  static int getDataSize(const char *& src, const char type);

  static int compareFields(const char *& src, const char *& dest, bool * order, int oLen);
  static int compareBags(const char *& src, const char srcType, const char *& dest,
      const char destType);
  static int compareInnerTuples(const char *& src, const char srcType, const char *& dest,
      const char destType);
  static int compareMaps(map<string *, const char *> & left, map<string *, const char *> & right);
  static void stringToBooleans(string & conf, bool *& order, int & len);

public:
  static int8_t ReadByte(const char * src);
  static uint8_t ReadUnsignedByte(const char * src);
  static int16_t ReadShort(const char * src);
  static uint16_t ReadUnsignedShort(const char * src);
  static int32_t ReadInt(const char * src);
  static int64_t ReadLong(const char * src);
  static float ReadFloat(const char * src);
  static double ReadDouble(const char * src);
  static void ReadString(const char *& src, string * ret);
  static void ReadUTF(const char *& src, string * ret);

  static void nextField(const char *& src);

  static int PigTextComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int PigBytesComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int PigTupleComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int PigNullableWritableComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength, ComparatorPtr comparator);
  static int PigSecondaryKeyComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);

  template<typename T>
  static int compare(const T & left, const T & right) {
    if (left < right)
      return -1;
    else if (right < left)
      return 1;
    else
      return 0;

  }

  static int compareBytes(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int compareInnerTuples(const char *& src, const char *& dest);
  static int compareTuples(const char *& src, const char *& dest);

  static char interSedesTypeToDataType(const char type);

  static void setPigWritableType();
  static PigWritableType getPigWritableType();
  static string getPackageName();
  static bool isGroupOnly();
  static bool useSecondaryKey();
  static void setSortOrder();
  static void setSecSort(bool b);
  static void setSecondarySortOrder();
  static bool getSortOrder();
};

bool tuplecmp(const char * lhs, const char * rhs);

} // namespace NativeTask

#endif /* PIGUTILS_H_ */
