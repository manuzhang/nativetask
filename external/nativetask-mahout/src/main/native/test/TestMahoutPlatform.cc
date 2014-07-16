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

#include <limits.h>
#include <list>
#include "test_commons.h"
#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"
#include "util/WritableUtils.h"

#include "MahoutPlatform.h"

#define bufferSize 512

template<typename T>
void writeNum(T num, char* pos, uint32_t & len) {
  len = sizeof(T);
  char* bytes = new char[len];
  *(T*)bytes = num;
  for (int i = len - 1; i >= 0; i--) {
    *(pos++) = bytes[i];
  }
  delete[] bytes;
}
;

template<typename T, typename WriteFunction>
void comparatorTest(T list1[], T list2[], int32_t result[], int length, ComparatorPtr comparator,
    WriteFunction writeFunction) {
  uint32_t srclength = sizeof(T);
  uint32_t destlength = sizeof(T);
  char* src = new char[srclength];
  char* dest = new char[destlength];
  for (int i = 0; i < length; i++) {
    memset(src, 0, srclength);
    memset(dest, 0, destlength);
    writeFunction(list1[i], src, srclength);
    writeFunction(list2[i], dest, destlength);
    ASSERT_EQ(result[i], comparator(src, srclength, dest, destlength));
  }
  delete[] src;
  delete[] dest;
}

TEST(NativeObjectFactory, ByteComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(ByteType, NULL);
  char num_list1[] = {-128, -1, 0, 1, 127};
  char num_list2[] = {-127, -2, 1, 0, 127};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &writeNum<char>);
}

TEST(NativeObjectFactory, IntComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(IntType, NULL);
  int32_t num_list1[] = {INT_MIN, -1, 0, 1, INT_MAX};
  int32_t num_list2[] = {INT_MIN + 1, -2, 1, 0, INT_MAX};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &writeNum<int32_t>);
}

TEST(NativeObjectFactory, LongComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(LongType, NULL);
  int64_t num_list1[] = {LLONG_MIN, -1, 0, 1, LLONG_MAX};
  int64_t num_list2[] = {LLONG_MIN + 1, -2, 1, 0, LLONG_MAX};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &writeNum<int64_t>);
}

TEST(NativeObjectFactory, FloatComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(FloatType, NULL);
  float num_list1[] = {-1.11f, -0.001f, 0, 1, 1.11f};
  float num_list2[] = {-1.1f, -0.002f, 0.001f, 0.9999f, 1.11f};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &writeNum<float>);
}

TEST(NativeObjectFactory, DoubleComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(DoubleType, NULL);
  double num_list1[] = {-1.11, -0.001, 0, 1, 1.11};
  double num_list2[] = {-1.1, -0.002, 0.001, 0.9999, 1.11};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &writeNum<double>);
}

TEST(NativeObjectFactory, VIntComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(VIntType, NULL);
  int32_t num_list1[] = {INT_MIN, -1, 0, 1, INT_MAX};
  int32_t num_list2[] = {INT_MIN + 1, -2, 1, 0, INT_MAX};
  int32_t result[] = {-1, 1, -1, 1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &WritableUtils::WriteVInt);
}

TEST(NativeObjectFactory, VLongComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(VLongType, NULL);
  int64_t num_list1[] = {LLONG_MIN, -1, 0, 1, LLONG_MAX};
  int64_t num_list2[] = {LLONG_MIN + 1, -2, 1, 0, LLONG_MAX};
  int32_t result[] = {-1, 1, -1, 1, 0};
  void (*f)(int64_t v, char * pos, uint32_t & len);
  f = WritableUtils::WriteVLong;
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator, f);
}

TEST(NativeObjectFactory, BytesComparator) {
  ComparatorPtr comparator = NativeTask::get_comparator(BytesType, NULL);
  char array1[] = "foo";
  char array2[] = "bar";
  char array3[] = "";
  char array4[] = "bar";
  ASSERT_EQ(1, comparator(array1, sizeof(array1), array2, sizeof(array2)));
  ASSERT_EQ(-1, comparator(array3, sizeof(array3), array2, sizeof(array2)));
  ASSERT_EQ(0, comparator(array2, sizeof(array2), array4, sizeof(array4)));
}

class StringTuple {
private:
  std::list<string> strings;
public:
  StringTuple* append(string s) {
    strings.push_back(s);
    return this;
  }

  void write(char* pos) {
    uint32_t intsize = 0;
    int size = strings.size();
    memset(pos, 0, bufferSize);
    writeNum(size, pos, intsize);
    pos += intsize;
    std::list<string>::iterator i;
    for (i = strings.begin(); i != strings.end(); ++i) {
      WritableUtils::WriteVInt(i->size(), pos, intsize);
      pos += intsize;
      memcpy(pos, i->c_str(), i->size());
      pos += i->size();
    }
  }

  void clear() {
    strings.clear();
  }
};

TEST(MahoutPlatform,StringTupleComparator) {
  ComparatorPtr comparator = &MahoutPlatform::StringTupleComparator;
  char* src = new char[bufferSize];
  char* dest = new char[bufferSize];
  uint32_t srclength;
  uint32_t destlength;
  string strings[] = {"", "bar", "foo", "go", "string", "tuple"};
  StringTuple* tuple1 = new StringTuple();
  StringTuple* tuple2 = new StringTuple();
  tuple1->append(strings[1])->append(strings[2]);
  tuple1->write(src);
  tuple2->write(dest);
  ASSERT_EQ(1, comparator(src, srclength, dest, destlength));

  tuple2->append(strings[1])->append(strings[2]);
  tuple2->write(dest);
  ASSERT_EQ(0, comparator(src, srclength, dest, destlength));

  tuple2->append(strings[3]);
  tuple2->write(dest);
  ASSERT_EQ(-1, comparator(src, srclength, dest, destlength));

  tuple1->clear();
  tuple1->append(strings[0])->append(strings[4]);
  tuple1->write(src);
  //since the src's first element length is 0 while the dest's is 3,
  //so the value returned by BytesComparator is -3
  ASSERT_EQ(-3, comparator(src, srclength, dest, destlength));

  delete tuple1;
  delete tuple2;
  delete[] src;
  delete[] dest;
}

TEST(MahoutPlatform, VarIntComparator) {
  ComparatorPtr comparator = &MahoutPlatform::VarIntComparator;
  int32_t num_list1[] = {INT_MIN, -1, 0, 1, 100, INT_MAX};
  int32_t num_list2[] = {INT_MIN + 1, 0, 1, 0, 101, INT_MAX};
  int32_t result[] = {-1, -1, -1, 1, -1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &MahoutPlatform::WriteSignedVarInt);
}

TEST(MahoutPlatform, VarLongComparator) {
  ComparatorPtr comparator = &MahoutPlatform::VarLongComparator;
  int64_t num_list1[] = {-100, -1, 0, 1, 100, 127};
  int64_t num_list2[] = {-101, 0, 1, 0, 101, 127};
  int32_t result[] = {1, -1, -1, 1, -1, 0};
  comparatorTest(num_list1, num_list2, result, sizeof(result) / sizeof(result[0]), comparator,
      &MahoutPlatform::WriteSignedVarLong);
}

inline int writeGram(char* pos, const char* array, uint32_t length) {
  uint32_t tmp = 0;
  memset(pos, 0, bufferSize);
  MahoutPlatform::WriteUnsignedVarInt(length, pos, tmp);
  memcpy(pos + tmp, array, length);
  return tmp + length;
}

TEST(MahoutPlatform, GramComparator) {
  ComparatorPtr comparator = &MahoutPlatform::GramComparator;
  char* src = new char[bufferSize];
  char* dest = new char[bufferSize];
  uint32_t srclength;
  uint32_t destlength;
  char array1[] = "foo";
  char array2[] = "bar";
  char array3[] = "";
  srclength = writeGram(src, array1, sizeof(array1));
  destlength = writeGram(dest, array2, sizeof(array2));
  ASSERT_EQ(1, comparator(src, srclength, dest, destlength));

  srclength = writeGram(src, array3, sizeof(array3));
  ASSERT_EQ(-1, comparator(src, srclength, dest, destlength));

  srclength = writeGram(src, array2, sizeof(array2));
  ASSERT_EQ(0, comparator(src, srclength, dest, destlength));

  delete[] src;
  delete[] dest;
}

inline void writeGramKey(char* pos, char* gram, char* order, uint32_t gramlength,
    uint32_t ordersize) {
  uint32_t gvarint = 0;
  uint32_t ovarint = 0;
  memset(pos, 0, bufferSize);
  MahoutPlatform::WriteUnsignedVarInt(gramlength + ordersize, pos, gvarint);
  MahoutPlatform::WriteUnsignedVarInt(gramlength, pos + gvarint, ovarint);
  memcpy(pos + gvarint + ovarint, gram, gramlength);
  memcpy(pos + gvarint + ovarint + gramlength, order, ordersize);
}

TEST(MahoutPlatform, GramKeyComparator) {
  ComparatorPtr comparator = &MahoutPlatform::GramKeyComparator;
  char* src = new char[bufferSize];
  char* dest = new char[bufferSize];
  uint32_t srclength;
  uint32_t destlength;
  char order1[0];
  char order2[1];
  order2[0] = 1;
  char array1[] = "foo";
  char array2[] = "bar";
  char array3[] = "";

  writeGramKey(src, array1, order1, sizeof(array1), sizeof(order1));
  writeGramKey(dest, array1, order2, sizeof(array1), sizeof(order2));
  ASSERT_EQ(-1, comparator(src, srclength, dest, destlength));

  writeGramKey(dest, array2, order1, sizeof(array2), sizeof(order1));
  ASSERT_EQ(1, comparator(src, srclength, dest, destlength));

  writeGramKey(dest, array3, order1, sizeof(array3), sizeof(order1));
  ASSERT_EQ(1, comparator(src, srclength, dest, destlength));

  writeGramKey(dest, array1, order1, sizeof(array1), sizeof(order1));
  ASSERT_EQ(0, comparator(src, srclength, dest, destlength));

  delete[] src;
  delete[] dest;
}

inline void writeSplitPartitioned(char* pos, uint32_t taskId, uint64_t itemOrdinal,
    uint32_t &length) {
  uint32_t tmp = 0;
  memset(pos, 0, bufferSize);
  MahoutPlatform::WriteUnsignedVarInt(taskId, pos, tmp);
  MahoutPlatform::WriteUnsignedVarLong(itemOrdinal, pos + tmp, length);
  length += tmp;
}

TEST(MahoutPlatform, SplitPartitionedComparator) {
  ComparatorPtr comparator = &MahoutPlatform::SplitPartitionedComparator;
  char* src = new char[bufferSize];
  char* dest = new char[bufferSize];
  uint32_t srclength;
  uint32_t destlength;
  uint32_t srcTaskId[] = {0, 1, 127, 127, UINT_MAX};
  uint64_t srcItemOrdinal[] = {0, 1, 0, 0, ULLONG_MAX};
  uint32_t destTaskId[] = {0, 1, 126, 128, UINT_MAX};
  uint64_t destItemOrdinal[] = {1, 0, 0, 0, ULLONG_MAX};
  int32_t result[] = {-1, 1, 1, -1, 0};
  for (uint32_t i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
    writeSplitPartitioned(src, srcTaskId[i], srcItemOrdinal[i], srclength);
    writeSplitPartitioned(dest, destTaskId[i], destItemOrdinal[i], destlength);
    ASSERT_EQ(result[i], comparator(src, srclength, dest, destlength));
  }
  delete[] src;
  delete[] dest;
}

inline void writeEntityEntity(char* pos, int64_t Id_a, int64_t Id_b, uint32_t &length) {
  uint32_t tmp = 0;
  memset(pos, 0, bufferSize);
  MahoutPlatform::WriteSignedVarLong(Id_a, pos, tmp);
  MahoutPlatform::WriteSignedVarLong(Id_b, pos + tmp, length);
  length += tmp;
}

TEST(MahoutPlatform, EntityEntityComparator) {
  ComparatorPtr comparator = &MahoutPlatform::EntityEntityComparator;
  char* src = new char[bufferSize];
  char* dest = new char[bufferSize];
  uint32_t srclength;
  uint32_t destlength;
  int64_t srcId_a[] = {LLONG_MIN, 0, 1, 127, LLONG_MAX};
  int64_t srcId_b[] = {LLONG_MIN, -1, 0, 0, LLONG_MAX};
  int64_t destId_a[] = {LLONG_MIN + 1, -1, 1, 127, LLONG_MAX};
  int64_t destId_b[] = {LLONG_MIN, 0, 1, -1, LLONG_MAX};
  int32_t result[] = {-1, 1, -1, 1, 0};
  for (uint32_t i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
    writeEntityEntity(src, srcId_a[i], srcId_b[i], srclength);
    writeEntityEntity(dest, destId_a[i], destId_b[i], destlength);
    ASSERT_EQ(result[i], comparator(src, srclength, dest, destlength));
  }
  delete[] src;
  delete[] dest;
}

