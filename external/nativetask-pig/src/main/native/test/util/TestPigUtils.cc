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

#include<climits>
#include<cfloat>
#include<ctime>
#include<iostream>
#include<sstream>

#include "commons.h"
#include "test_commons.h"
#include "lib/BufferStream.h"
#include "lib/NativeObjectFactory.h"
#include "util/PigUtils.h"
#include "util/WritableUtils.h"

using std::cout;
using std::endl;
using std::map;
using std::min;
using std::ostringstream;
using std::string;

void TestReadByte(int8_t * array, int len);
void TestReadUnsignedByte(uint8_t * array, int len);
void TestReadShort(int16_t * array, int len);
void TestReadUnsignedShort(uint16_t * array, int len);
void TestReadInt(int32_t * array, int len);
void TestReadLong(int64_t * array, int len);
void TestReadFloat(float * array, int len);
void TestReadDouble(double * array, int len);
template<typename T> void check(map<T, T> & checks);

void writeLong(int64_t l, string & data);
void writeInt(int32_t i, string & data);
void writeShort(int16_t s, string & data);
void writeByte(int8_t b, string & data);
void writeFloat(float f, string & data);
void writeDouble(double d, string & data);
void writeString(uint16_t src[], int len, string & ret);
void writeUTF(uint16_t src[], int len, string & ret);
void writeVInt(int i, string & ret);

char genPigTupleField(string & ret, int r = -1);
// methods to generate tuple field
void genPigNull(string & ret);
bool genPigBool(string & ret, int r = -1);
int genPigInt(string & ret, int r = -1);
long genPigLong(string & ret, int r = -1);
float genPigFloat(string & ret);
double genPigDouble(string & ret);
long genPigDateTime(string & ret);
string * genPigCharArray(string & ret, int r = -1);
string * genPigText(string & ret);
string * genPigByteArray(string & ret, int r = -1);
string * genPigBytes(string & ret);
char genPigByte(string & ret);
size_t genPigInternalMap(string & ret);
size_t genPigMap(string & ret, int r = -1);
void genPigMap(map<string *, string *> & m, string & ret);
size_t genPigTuple(string & ret, int r = -1);
void genPigTuple(vector<string *> & fields, string & ret);
string * genPigTupleOne(string & ret, int r = -1);
void genPigTupleOne(string & field, string & ret);
size_t genPigBag(string & ret, int r = -1);
void genPigBag(vector<string *> & tuples, string & ret);

char genPigWritable(string & value, string & wrt);

void unsignedCharsToString(uint16_t src[], int len, string & ret);
char getPigSedesType(const char * field);
ComparatorPtr getComparator(string writableType = ".NullableIntWritable");
char getPigSedesType(const char * field);
int getCompareResult(ComparatorPtr comparator, string & lhs, string & rhs);

template <typename T>
void compareWithOrder(T lv, T rv, string & lw, string & rw, string & cmp, string & order); 
 
template <typename T>
void compareAsc(T lv, T rv, string & lw, string & rw, string & cmp);
template <typename T>
void compareDesc(T lv, T rv, string & lw, string & rw, string & cmp);
void compareBytes(string & lw, string & rw, string & cmp);

Config & config = NativeObjectFactory::GetConfig();
time_t seed = time(NULL);


void unsignedCharsToString(uint16_t src[], int len, string & ret) {
  for (int i = 0; i < len; i++) {
    const uint16_t c = src[i];
    if (c > 0xff) {
      writeByte((c >> 8), ret);
    }
    writeByte((0xff & c), ret);
  }
}

void writeByte(int8_t b, string & data) {
  data.push_back(b);
}

void writeShort(int16_t s, string & data) {
  char buff[2];
  OutputBuffer ob(buff, 2);
  WritableUtils::WriteShort(&ob, s);
  data.append(buff, 2);
}

void writeInt(int32_t i, string & data) {
  char buff[4];
  OutputBuffer ob(buff, 4);
  WritableUtils::WriteInt(&ob, i);
  data.append(buff, 4);
}

void writeLong(int64_t l, string & data) {
  char buff[8];
  OutputBuffer ob(buff, 8);
  WritableUtils::WriteLong(&ob, l);
  data.append(buff, 8);
}

void writeFloat(float f, string & data) {
  char buff[4];
  OutputBuffer ob(buff, 4);
  WritableUtils::WriteFloat(&ob, f);
  data.append(buff, 4);
}

void writeDouble(double d, string & data) {
  char buff[8];
  OutputBuffer ob(buff, 8);
  uint64_t be = bswap64(*(uint64_t *) &d);
  ob.write(&be, 8);
  data.append(buff, 8);
}

void writeString(uint16_t src[], int len, string & ret) {
  string * data = new string();
  unsignedCharsToString(src, len, *data);
  int size = data->length();
  int swap = bswap(size);
  const char * pl = (char *) &swap;
  for (int i = 0; i < 4; i++) {
    writeByte(pl[i], ret);
  }
  ret.append(*data);
}

void writeUTF(uint16_t src[], int len, string & ret) {
  string * data = new string();
  uint16_t size = 0;
  for (int i = 0; i < len; i++) {
    const uint16_t c = src[i];
    if (c > 0 && c <= 0x7f) {
      data->push_back((char) c);
      size++;
    } else if (c == 0 || (c > 0x7f && c <= 0x7ff)) {
      data->push_back((char) (0xc0 | (0x1f & (c >> 6))));
      data->push_back((char) (0x80 | (0x3f & c)));
      size += 2;
    } else if (c > 0x7ff && c <= 0xffff) {
      data->push_back((char) (0xe0 | (0x0f & (c >> 12))));
      data->push_back((char) (0x80 | (0x3f & (c >> 6))));
      data->push_back((char) (0x80 | (0x3f & c)));
      size += 3;
    }
  }
  writeShort(size, ret);
  ret.append(*data);
}

void writeVInt(int i, string & ret) {
  char buff[10];
  unsigned int len = 0;
  WritableUtils::WriteVInt(i, buff, len);
  for (unsigned int j = 0; j < len; j++) {
    writeByte(buff[j], ret);
  }
}

void genPigNull(string & ret) {
  ret.clear();
  writeByte(PIG_NULL, ret);
}

bool genPigBool(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r)
    r = random.next_uint32() % 2;
  switch (r) {
    case PIG_BOOLEAN_TRUE:
      writeByte(PIG_BOOLEAN_TRUE, ret);
      break;
    case PIG_BOOLEAN_FALSE:
      writeByte(PIG_BOOLEAN_FALSE, ret);
      break;
  }
  if (0 == r) {
    return true;
  } else {
    return false;
  }
}

int genPigInt(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r)
    r = random.next_uint32() % 5 + 3;
  int i = random.next_int32();
  switch (r) {
    case PIG_INTEGER_0:
      writeByte(PIG_INTEGER_0, ret);
      i = 1;
      break;
    case PIG_INTEGER_1:
      writeByte(PIG_INTEGER_1, ret);
      i = 1;
      break;
    case PIG_INTEGER_INBYTE:
      writeByte(PIG_INTEGER_INBYTE, ret);
      i = (int8_t)i;
      writeByte(i, ret);
      break;
    case PIG_INTEGER_INSHORT:
      writeByte(PIG_INTEGER_INSHORT, ret);
      i = (int16_t)i;
      writeShort(i, ret);
      break;
    case PIG_INTEGER:
      writeByte(PIG_INTEGER, ret);
      i = (int32_t)i;
      writeInt(i, ret);
      break;
  }
  return i;
}

long genPigLong(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = random.next_uint32() % 6 + 31;
  }

  long l = random.next_uint64();
  switch (r) {
    case PIG_LONG_0:
      l = 0;
      writeByte(PIG_LONG_0, ret);
      break;
    case PIG_LONG_1:
      l = 1;
      writeByte(PIG_LONG_1, ret);
      break;
    case PIG_LONG_INBYTE:
      writeByte(PIG_LONG_INBYTE, ret);
      l = (int8_t) l;
      writeByte(l, ret);
      break;
    case PIG_LONG_INSHORT:
      writeByte(PIG_LONG_INSHORT, ret);
      l = (int16_t) l;
      writeShort(l, ret);
      break;
    case PIG_LONG_ININT:
      writeByte(PIG_LONG_ININT, ret);
      l = (int32_t) l;
      writeInt(l, ret);
      break;
    case PIG_LONG:
      writeByte(PIG_LONG, ret);
      l = (int64_t) l;
      writeLong(l, ret);
      break;
  }
  return l;
}

float genPigFloat(string & ret) {
  ret.clear();
  Random random(++seed);
  float f = random.nextFloat();
  writeByte(PIG_FLOAT, ret);
  writeFloat(f, ret);
  return f;
}

double genPigDouble(string & ret) {
  ret.clear();
  Random random(++seed);
  double d = random.nextDouble();
  writeByte(PIG_DOUBLE, ret);
  writeDouble(d, ret);
  return d;
}

string * genPigCharArray(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = random.next_uint32() % 2 + 14;
  }
  string * data = new string();
  switch (r) {
    case PIG_SMALLCHARARRAY: {
      size_t len = random.next_uint32() % (USHRT_MAX / 3);
      uint16_t * s = new uint16_t[len];
      for (size_t i = 0; i < len; i++) {
        s[i] = (uint16_t) random.next_uint32();
      }
      writeByte(PIG_SMALLCHARARRAY, ret);
      writeUTF(s, len, *data);
      ret.append(*data);
      delete s;
      break;
    }
    case PIG_CHARARRAY: {
      size_t len = random.next_uint32() % UCHAR_MAX + USHRT_MAX / 3;
      uint16_t * s = new uint16_t[len];
      for (size_t i = 0; i < len; i++) {
        s[i] = (uint16_t) random.next_uint32();
      }
      writeByte(PIG_CHARARRAY, ret);
      writeString(s, len, *data);
      ret.append(*data);
      delete s;
      break;
    }
  }
  return data;
}

string * genPigText(string & ret) {
  ret.clear();
  Random random(++seed);
  uint32_t len = random.next_uint32() % 5;
  string * s = new string();
  for (uint32_t i = 0; i < len; i++) {
    s->push_back((char) random.next_uint32());
  }
  writeVInt(len, ret);
  ret.append(*s);
  return s;
}

string * genPigByteArray(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = random.next_uint32() % 3 + 11;
  }
  size_t len = 0;
  switch (r) {
    case PIG_TINYBYTEARRAY:
      writeByte(PIG_TINYBYTEARRAY, ret);
      len = random.next_int32() % UCHAR_MAX;
      writeByte(len, ret);
      break;
    case PIG_SMALLBYTEARRAY:
      writeByte(PIG_SMALLBYTEARRAY, ret);
      len = random.next_int32() % UCHAR_MAX + UCHAR_MAX;
      writeShort(len, ret);
      break;
    case PIG_BYTEARRAY:
      writeByte(PIG_BYTEARRAY, ret);
      len = random.next_int32() % UCHAR_MAX + USHRT_MAX;
      writeInt(len, ret);
      break;
  }
  string * bytes = new string();
  for (size_t i = 0; i < len; i++) {
    uint8_t b = (uint8_t) random.next_uint32();
    bytes->push_back(b);
    writeByte(b, ret);
  }
  return bytes;
}

char genPigTupleField(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = -(random.next_uint32() % 14 + 2);
  }
  switch (r) {
    case -2:
    case PigNullType:
      genPigNull(ret);
      break;
    case -3:
    case PigByteArrayType:
      genPigByteArray(ret);
      break;
    case -4:
    case PigCharArrayType:
      genPigCharArray(ret);
      break;
    case -5:
    case PigTupleType:
      genPigTuple(ret);
      break;
    case -6:
    case PigBagType:
      genPigBag(ret);
      break;
    case -7:
    case PigIntType:
      genPigInt(ret);
      break;
    case -8:
    case PigLongType:
      genPigLong(ret);
      break;
    case -9:
    case PigInternalMapType:
      genPigInternalMap(ret);
      break;
    case -10:
    case PigMapType:
      genPigMap(ret);
      break;
    case -11:
    case PigFloatType:
      genPigFloat(ret);
      break;
    case -12:
    case PigDoubleType:
      genPigDouble(ret);
      break;
    case -13:
    case PigBoolType:
      genPigBool(ret);
      break;
    case -14:
    case PigDateTimeType:
      genPigDateTime(ret);
      break;
    case -15:
    case PigByteType:
      genPigByte(ret);
      break;
  }
  // return type
  return ret.at(0);
}

char genPigByte(string & ret) {
  ret.clear();
  writeByte(PIG_BYTE, ret);
  Random random(++seed);
  char byte = (char) random.next_uint32();
  writeByte(byte, ret);
  return byte;
}

string * genPigBytes(string & ret) {
  ret.clear();
  vector<string *> * fields = new vector<string *>();
  string * field = new string();
  genPigTupleField(*field);
  fields->push_back(field);
  genPigTuple(*fields, ret);
  return field;
}

long genPigDateTime(string & ret) {
  ret.clear();
  writeByte(PIG_DATETIME, ret);
  Random random(++seed);
  long mills = random.next_uint64();
  writeLong(mills, ret);
  short offs = (short) random.next_uint32();
  writeShort(offs, ret);
  return mills;
}

size_t genPigInternalMap(string & ret) {
  ret.clear();
  writeByte(PIG_INTERNALMAP, ret);

  Random random(++seed);
  size_t size = random.next_uint32() % 10;
  for (size_t i = 0; i < size; i++) {
    string * key = new string();
    string * value = new string();
    genPigTupleField(*key);
    genPigTupleField(*value);
    ret.append(*key);
    ret.append(*value);
  }
  return size;
}

size_t genPigMap(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 != r) {
    r = random.next_uint32() % 3 + 16;
  }
  size_t size = 0;
  switch (r) {
    case PIG_TINYMAP:
      writeByte(PIG_TINYMAP, ret);
      size = random.next_uint32() % UCHAR_MAX;
      writeByte(size, ret);
      break;
    case PIG_SMALLMAP:
      writeByte(PIG_SMALLMAP, ret);
      size = random.next_uint32() % UCHAR_MAX + UCHAR_MAX;
      writeShort(size, ret);
      break;
    case PIG_MAP:
      writeByte(PIG_MAP, ret);
      size = random.next_uint32() % UCHAR_MAX + USHRT_MAX;
      writeInt(size, ret);
      break;
  }
  for (size_t i = 0; i < size; i++) {
    string * key = new string();
    string * value = new string();
    genPigCharArray(*key);
    genPigTupleField(*value);
    ret.append(*key);
    ret.append(*value);
  }
  return size;
}

void genPigMap(map<string *, string *> & m, string & ret) {
  ret.clear();
  size_t size = m.size();

  if (size < UCHAR_MAX) {
    writeByte(PIG_TINYMAP, ret);
    writeByte(size, ret);
  } else if (size < USHRT_MAX) {
    writeByte(PIG_SMALLMAP, ret);
    writeShort(size, ret);
  } else {
    writeByte(PIG_MAP, ret);
    writeInt(size, ret);
  }
  for (map<string *, string *>::iterator it = m.begin(); it != m.end(); it++) {
    ret.append(*(it->first));
    ret.append(*(it->second));
  }
}

size_t genPigTuple(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = -(random.next_uint32() % 13 + 2);
  }
  size_t size = 0;
  switch (r) {
    case -2:
    case PIG_TUPLE_0:
      writeByte(PIG_TUPLE_0, ret);
      size = 0;
      break;
    case -3:
    case PIG_TUPLE_1:
      writeByte(PIG_TUPLE_1, ret);
      size = 1;
      break;
    case -4:
    case PIG_TUPLE_2:
      writeByte(PIG_TUPLE_2, ret);
      size = 2;
      break;
    case -5:
    case PIG_TUPLE_3:
      writeByte(PIG_TUPLE_3, ret);
      size = 3;
      break;
    case -6:
    case PIG_TUPLE_4:
      writeByte(PIG_TUPLE_4, ret);
      size = 4;
      break;
    case -7:
    case PIG_TUPLE_5:
      writeByte(PIG_TUPLE_5, ret);
      size = 5;
      break;
    case -8:
    case PIG_TUPLE_6:
      writeByte(PIG_TUPLE_6, ret);
      size = 6;
      break;
    case -9:
    case PIG_TUPLE_7:
      writeByte(PIG_TUPLE_7, ret);
      size = 7;
      break;
    case -10:
    case PIG_TUPLE_8:
      writeByte(PIG_TUPLE_8, ret);
      size = 8;
      break;
    case -11:
    case PIG_TUPLE_9:
      writeByte(PIG_TUPLE_9, ret);
      size = 9;
      break;
    case -12:
    case PIG_TINYTUPLE:
      writeByte(PIG_TINYTUPLE, ret);
      writeByte(size, ret);
      size = random.next_uint32() % UCHAR_MAX;
      break;
    case -13:
    case PIG_SMALLTUPLE:
      writeByte(PIG_SMALLTUPLE, ret);
      writeShort(size, ret);
      size = random.next_uint32() % UCHAR_MAX + UCHAR_MAX;
      break;
    case -14:
    case PIG_TUPLE:
      writeByte(PIG_TUPLE, ret);
      writeInt(size, ret);
      size = random.next_uint32() % UCHAR_MAX + USHRT_MAX;
      break;
  }
  for (size_t i = 0; i < size; i++) {
    string * field = new string();
    genPigTupleField(*field);
    ret.append(*field);
  }
  return size;
}

string * genPigTupleOne(string & ret, int r) {
  ret.clear();
  string * field = new string();
  genPigTupleField(*field, r);
  genPigTupleOne(*field, ret);
  return field;
}

void genPigTupleOne(string & field, string & ret) {
  ret.clear();
  vector<string *> * fields = new vector<string *>();
  fields->push_back(&field);
  genPigTuple(*fields, ret);
}

void genPigTuple(vector<string *> & fields, string & ret) {
  ret.clear();
  size_t size = fields.size();
  switch (size) {
    case 0:
      writeByte(PIG_TUPLE_0, ret);
      break;
    case 1:
      writeByte(PIG_TUPLE_1, ret);
      break;
    case 2:
      writeByte(PIG_TUPLE_2, ret);
      break;
    case 3:
      writeByte(PIG_TUPLE_3, ret);
      break;
    case 4:
      writeByte(PIG_TUPLE_4, ret);
      break;
    case 5:
      writeByte(PIG_TUPLE_5, ret);
      break;
    case 6:
      writeByte(PIG_TUPLE_6, ret);
      break;
    case 7:
      writeByte(PIG_TUPLE_7, ret);
      break;
    case 8:
      writeByte(PIG_TUPLE_8, ret);
      break;
    case 9:
      writeByte(PIG_TUPLE_9, ret);
      break;
    default:
      if (size < UCHAR_MAX) {
        writeByte(PIG_TINYTUPLE, ret);
        writeByte(size, ret);
      } else if (size < USHRT_MAX) {
        writeByte(PIG_SMALLTUPLE, ret);
        writeShort(size, ret);
      } else {
        writeByte(PIG_TUPLE, ret);
        writeInt(size, ret);
      }
  }
  for (size_t i = 0; i < size; i++) {
    ret.append(*fields.at(i));
  }
}

size_t genPigBag(string & ret, int r) {
  ret.clear();
  Random random(++seed);
  if (-1 == r) {
    r = random.next_uint32() % 3 + 22;
  }
  uint64_t size = 0;
  switch (r) {
    case PIG_TINYBAG:
      writeByte(PIG_TINYBAG, ret);
      size = random.next_uint32() % UCHAR_MAX;
      writeByte(size, ret);
      break;
    case PIG_SMALLBAG:
      writeByte(PIG_SMALLBAG, ret);
      size = random.next_uint32() % UCHAR_MAX + UCHAR_MAX;
      writeShort(size, ret);
      break;
    case PIG_BAG:
      writeByte(PIG_BAG, ret);
      size = random.next_uint32() % UCHAR_MAX + USHRT_MAX;
      writeLong(size, ret);
      break;
  }
  for (uint64_t i = 0; i < size; i++) {
    string * tuple = new string();
    genPigTuple(*tuple);
    ret.append(*tuple);
  }
  return size;
}

void genPigBag(vector<string *> & tuples, string & ret) {
  ret.clear();
  size_t size = tuples.size();
  if (size < UCHAR_MAX) {
    writeByte(PIG_TINYBAG, ret);
    writeByte(size, ret);
  } else if (size < USHRT_MAX) {
    writeByte(PIG_SMALLBAG, ret);
    writeShort(size, ret);
  } else {
    writeByte(PIG_BAG, ret);
    writeLong(size, ret);
  }
  for (size_t i = 0; i < size; i++) {
    ret.append(*tuples.at(i));
  }
}

char genPigWritable(string & value, string & wrt) {
  wrt.clear();
  writeByte(false, wrt);
  wrt.append(value);
  Random random(++seed);
  char index = random.next_uint32() & 0x7f;
  writeByte(index, wrt);
  return index;
}

char getPigSedesType(const char * field) {
  return field[0];
}

ComparatorPtr getComparator(string writableType) {
  PigUtils::setPigWritableType();
  Config & config = NativeObjectFactory::GetConfig();
  string key_class = PigUtils::getPackageName() + writableType;
  config.set(MAPRED_MAPOUTPUT_KEY_CLASS, key_class);
  string clazz(config.get(MAPRED_MAPOUTPUT_KEY_CLASS));
  assert(key_class == clazz);
  return NativeTask::get_comparator(PigType, NULL);
}

int getCompareResult(ComparatorPtr comparator, string & lhs, string & rhs) {
  int c = comparator(lhs.c_str(), lhs.length(), rhs.c_str(), rhs.length());
  if (c < 0)
    return -1;
  else if (c > 0)
    return 1;
  else
    return 0;
}




/************ Tests ******************/

void TestReadByte(int8_t * array, int len) {
  for (int i = 0; i < len; i++) {
    int8_t expected = array[i];
    int8_t actual = PigUtils::ReadByte((char *) &array[i]);
    ASSERT_EQ(expected, actual);
  }
}

void TestReadUnsignedByte(uint8_t * array, int len) {
  for (int i = 0; i < len; i++) {
    int8_t expected = array[i];
    int8_t actual = PigUtils::ReadUnsignedByte((char *) &array[i]);
    ASSERT_EQ(expected, actual);
  }
}

void TestReadShort(int16_t * array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    int16_t expected = array[i];
    writeShort(array[i], *data);
    int16_t actual = PigUtils::ReadShort(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

void TestReadUnsignedShort(uint16_t * array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    uint16_t expected = array[i];
    writeShort(array[i], *data);
    uint16_t actual = PigUtils::ReadUnsignedShort(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

void TestReadLong(int64_t * array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    int64_t expected = array[i];
    writeLong(array[i], *data);
    int64_t actual = PigUtils::ReadLong(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

void TestReadInt(int32_t * array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    int32_t expected = array[i];
    writeInt(array[i], *data);
    int32_t actual = PigUtils::ReadInt(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

void TestReadFloat(float * array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    float expected = array[i];
    writeFloat(array[i], *data);
    float actual = PigUtils::ReadFloat(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

void TestReadDouble(double* array, int len) {
  for (int i = 0; i < len; i++) {
    string * data = new string();
    double expected = array[i];
    writeDouble(array[i], *data);
    double actual = PigUtils::ReadDouble(data->c_str());
    ASSERT_EQ(expected, actual);
    delete data;
  }
}

template<typename T>
void check(map<T, T> & checks) {
  for (typename map<T, T>::iterator it = checks.begin(); it != checks.end(); it++) {
    T expected = it->first;
    T actual = it->second;
    ASSERT_EQ(expected, actual);
  }
}

TEST(PigUtils, ReadByte) {
  Random random(++seed);
  int8_t byteArray[] = { (int8_t) random.next_uint32(), CHAR_MAX, CHAR_MIN };
  TestReadByte(byteArray, sizeof byteArray / sizeof(int8_t));
}

TEST(PigUtils, ReadUnsignedByte) {
  Random random(++seed);
  uint8_t ubyteArray[] = { (uint8_t) random.next_uint32(), 0, UCHAR_MAX, CHAR_MIN };
  TestReadUnsignedByte(ubyteArray, sizeof ubyteArray / sizeof(uint8_t));
}

TEST(PigUtils, ReadShort) {
  Random random(++seed);
  int16_t shortArray[] = { (int16_t) random.next_int32(), SHRT_MAX, SHRT_MIN };
  TestReadShort(shortArray, sizeof shortArray / sizeof(int16_t));
}

TEST(PigUtils, ReadUnsignedShort) {
  Random random(++seed);
  uint16_t ushortArray[] = { (uint16_t) random.next_int32(), 0, USHRT_MAX, SHRT_MIN };
  TestReadUnsignedShort(ushortArray, sizeof ushortArray / sizeof(uint16_t));
}

TEST(PigUtils, ReadInt) {
  Random random(++seed);
  int32_t intArray[] = { random.next_int32(), INT_MAX, INT_MIN };
  TestReadInt(intArray, sizeof intArray / sizeof(int32_t));
}

TEST(PigUtils, ReadLong) {
  Random random(++seed);
  int64_t longArray[] = { (int64_t) random.next_uint64(), LONG_MAX, LONG_MIN };
  TestReadLong(longArray, sizeof longArray / sizeof(int64_t));
}

TEST(PigUtils, ReadFloat) {
  Random random(++seed);
  float floatArray[] = { random.nextFloat(), FLT_MAX, FLT_MIN };
  TestReadFloat(floatArray, sizeof floatArray / sizeof(float));
}

TEST(PigUtils, ReadDouble) {
  Random random(++seed);
  double doubleArray[] = { random.nextDouble(), DBL_MAX, DBL_MIN };
  TestReadDouble(doubleArray, sizeof doubleArray / sizeof(double));
}

TEST(PigUtils, ReadString) {
  uint16_t src[] = { 0x7f, 0x7ff, 0 };
  int len = sizeof src / sizeof(uint16_t);
  string * expected = new string();
  string * actual = new string();
  string * ret = new string();

  unsignedCharsToString(src, len, *expected);
  writeString(src, len, *ret);
  const char * pe = ret->c_str();
  PigUtils::ReadString(pe, actual);
  ASSERT_EQ(*expected, *actual);

  delete ret;
  delete expected;
  delete actual;
}

TEST(PigUtils, ReadUTF) {
  uint16_t src[] = { 0x7f, 0x80, 0x800, 0x281, 0 };
  int len = sizeof src / sizeof(uint16_t);
  string * expected = new string();
  string * actual = new string();
  string * ret = new string();

  unsignedCharsToString(src, len, *expected);
  writeUTF(src, len, *ret);
  const char * ps = ret->c_str();
  PigUtils::ReadUTF(ps, actual);
  ASSERT_EQ(*expected, *actual);

  delete ret;
  delete expected;
  delete actual;
}

void compareBytes(string & lw, string & rw, string & cmp) {
  config.setBool("native.pig.groupOnly", true);
  ComparatorPtr comparator = getComparator(cmp);
  ASSERT_EQ(true, comparator != NULL);
  ASSERT_EQ(
      PigUtils::compareBytes(lw.c_str(), lw.length(), rw.c_str(), rw.length()),
      getCompareResult(comparator, lw, rw)
  );
}

template <typename T>
void compareWithOrder(T lv, T rv, string & lw, string & rw, string & cmp, string & order) {
  config.setBool("native.pig.groupOnly", false);
  config.set("native.pig.sortOrder", order);
  ComparatorPtr comparator = getComparator(cmp);
  ASSERT_EQ(true, comparator != NULL);
  ASSERT_EQ(PigUtils::compare<T>(lv, rv), getCompareResult(comparator, lw, rw));
}

template <typename T>
void compareAsc(T lv, T rv, string & lw, string & rw, string & cmp) {
  string order = "1";
  compareWithOrder(lv, rv, lw, rw, cmp, order);
}

template <typename T>
void compareDesc(T lv, T rv, string & lw, string & rw, string & cmp) {
  string order = "0";
  compareWithOrder(rv, lv, lw, rw, cmp, order);
}

TEST(NativeObjectFactory, PigSecondaryKeyComparator) {
  config.setBool("native.pig.useSecondaryKey", true);
  config.set("native.pig.secondarySortOrder", "0");

  ComparatorPtr comparator = getComparator();
  ASSERT_EQ(true, comparator != NULL);

  Random random(++seed);
  string * mq1 = new string();
  string * mq2 = new string();
  mq1->push_back(random.next_uint32() % 2);
  char mqIndex1 = 0x80 | (0xff & random.next_uint32());
  mq1->push_back(mqIndex1);
  mq2->push_back(random.next_uint32() % 2);
  char mqIndex2 = 0x80 | (0xff & random.next_uint32());
  mq2->push_back(mqIndex2);
  // if multiquery, compare index
  ASSERT_EQ(PigUtils::compare<char>((mqIndex1 & 0x7f), (mqIndex2 & 0x7f)),
      getCompareResult(comparator, *mq1, *mq2));

  string * null1 = new string();
  string * null2 = new string();
  null1->push_back(true);
  char nullIndex1 = random.next_uint32() & 0x7f;
  null1->push_back(nullIndex1);
  null2->push_back(true);
  char nullIndex2 = random.next_uint32() & 0x7f;
  null2->push_back(nullIndex2);
  // if both null, compare index
  ASSERT_EQ(PigUtils::compare<char>(nullIndex1, nullIndex2),
      getCompareResult(comparator, *null1, *null2));


  delete mq1;
  delete mq2;
  delete null2;

  vector<string *> * d1 = new vector<string *>();
  vector<string *> * d2 = new vector<string *>();

  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  string * int1 = new string();
  genPigInt(*int1, PIG_INTEGER);
  string * int2 = new string(*int1);
  string * float1 = new string();
  float f1 = genPigFloat(*float1);
  string * float2 = new string();
  float f2 = genPigFloat(*float2);
  ASSERT_EQ(*int1, *int2);

  d1->push_back(int1);
  d1->push_back(float1);
  d2->push_back(int2);
  d2->push_back(float2);

  genPigTuple(*d1, *v1);
  genPigTuple(*d2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  ASSERT_EQ(-1, getCompareResult(comparator, *null1, *wrt1));
  // order PigWritable by int asc float desc
  ASSERT_EQ(PigUtils::compare<float>(f2, f1), getCompareResult(comparator, *wrt1, *wrt2));

  delete null1;
  delete float1;
  delete float2;
  delete d1;
  delete d2;
  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;

  string * v3 = new string();
  string * v4 = new string();
  string * wrt3 = new string();
  string * wrt4 = new string();
  vector<string *> * d3 = new vector<string *>();
  vector<string *> * d4 = new vector<string *>();

  string * n3 = new string();
  string * n4 = new string();
  genPigNull(*n3);
  genPigNull(*n4);
  d3->push_back(n3);
  d3->push_back(int1);
  d4->push_back(n4);
  d4->push_back(int2);
  genPigTuple(*d3, *v3);
  genPigTuple(*d4, *v4);

  char idx1 = genPigWritable(*v3, *wrt3);
  char idx2 = genPigWritable(*v4, *wrt4);

  ASSERT_EQ(PigUtils::compare<char>(idx1, idx2), getCompareResult(comparator, *wrt3, *wrt4));

  delete int1;
  delete int2;
  delete n3;
  delete n4;
  delete d3;
  delete d4;
  delete v3;
  delete v4;
  delete wrt3;
  delete wrt4;

}

TEST(NativeObjectFactory, PigNullableBooleanComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  bool b1 = true;
  bool b2 = false;
  writeByte(b1, *v1);
  writeByte(b2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableBooleanWritable";
  compareAsc<bool>(b1, b2, *wrt1, *wrt2, name);
  compareDesc<bool>(b1, b2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigNullableIntComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  Random random(++seed);
  int i1 = random.next_int32();
  int i2 = random.next_int32();
  writeInt(i1, *v1);
  writeInt(i2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableIntWritable";
  compareAsc<int>(i1, i2, *wrt1, *wrt2, name);
  compareDesc<int>(i1, i2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigNullableLongComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  Random random(++seed);
  long l1 = random.next_uint64();
  long l2 = random.next_uint64();
  writeLong(l1, *v1);
  writeLong(l2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableLongWritable";
  compareAsc<long>(l1, l2, *wrt1, *wrt2, name);
  compareDesc<long>(l1, l2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigNullableFloatComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  Random random(++seed);
  float f1 = random.nextFloat();
  float f2 = random.nextFloat();
  writeFloat(f1, *v1);
  writeFloat(f2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableFloatWritable";
  compareAsc<float>(f1, f2, *wrt1, *wrt2, name);
  compareDesc<float>(f1, f2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigNullableDoubleComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  Random random(++seed);
  double d1 = random.nextDouble();
  double d2 = random.nextDouble();
  writeDouble(d1, *v1);
  writeDouble(d2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableDoubleWritable";
  compareAsc<double>(d1, d2, *wrt1, *wrt2, name);
  compareDesc<double>(d1, d2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;

}

TEST(NativeObjectFactory, PigNullableDateTimeComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  Random random(++seed);
  int64_t time1 = random.next_uint64();
  int16_t off1 = (int16_t) random.next_uint32();
  writeLong(time1, *v1);
  writeShort(off1, *v1);
  int64_t time2 = random.next_uint64();
  int16_t off2 = (int16_t) random.next_uint32();
  writeLong(time2, *v2);
  writeShort(off2, *v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableDateTimeWritable";
  compareAsc<long>(time1, time2, *wrt1, *wrt2, name);
  compareDesc<long>(time1, time2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigTextComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  string * s1 = genPigText(*v1);
  string * s2 = genPigText(*v2);

  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableText";
  compareAsc<string>(*s1, *s2, *wrt1, *wrt2, name);
  compareDesc<string>(*s1, *s2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete s1;
  delete s2;
  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}

TEST(NativeObjectFactory, PigTupleComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string name = ".NullableTuple";
 /* Random random(time(NULL));
    vector<char> * typeArray = new vector<char>();
   vector<string *> * fields = new vector<string *>();
   char tupleTypes[] = { PIG_TUPLE_0, PIG_TUPLE_1, PIG_TUPLE_2, PIG_TUPLE_3, PIG_TUPLE_4,
   PIG_TUPLE_5, PIG_TUPLE_6, PIG_TUPLE_7, PIG_TUPLE_8, PIG_TUPLE_9, PIG_TINYTUPLE,
   PIG_SMALLTUPLE, PIG_TUPLE };

   for (size_t i = 0; i < sizeof tupleTypes / sizeof(char); i++) {
   if (i >= 1 && i <= 9) {
   string * field = new string();
   typeArray->push_back(genPigTupleField(*field));
   fields->push_back(field);
   } else if (10 == i) {
   size_t size = random.next_int32() % UCHAR_MAX;
   for (size_t i = 0; i < size; i++) {
   string * field = new string();
   genPigTupleField(*field);
   fields->push_back(field);
   }
   } else if (11 == i) {
   size_t size = random.next_int32() % UCHAR_MAX + UCHAR_MAX;
   for (size_t i = 0; i < size; i++) {
   string * field = new string();
   genPigTupleField(*field);
   fields->push_back(field);
   }
   } else if (12 == i) {
   size_t size = random.next_int32() % UCHAR_MAX + USHRT_MAX;
   for (size_t i = 0; i < size; i++) {
   string * field = new string();
   genPigTupleField(*field);
   fields->push_back(field);
   }
   }
   string * tuple = new string();
   genPigTuple(*fields, *tuple);
   ASSERT_EQ(tupleTypes[i], getPigSedesType(tuple->c_str()));

   if (9 == i) {   // PIG_TUPLE_9
   ASSERT_EQ(PIG_TUPLE_9, tuple->c_str()[0]);
   const char * pos = tuple->c_str() + 1;
   for (int j = 0; j < 9; j++) {
   ASSERT_EQ(typeArray->at(j), getPigSedesType(pos));
   PigUtils::nextField(pos);
   }
   }
   }

   delete typeArray;
   delete fields;
*/
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();
  vector<string *> * d1 = new vector<string *>();
  vector<string *> * d2 = new vector<string *>();

  string * int1 = new string();
  int i1 = genPigInt(*int1);
  d1->push_back(int1);

  genPigTuple(*d1, *v1);
  genPigTuple(*d2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  // wrt1.size > wrt2.size => wrt1 > wrt2
  compareAsc<int>(d1->size(), d2->size(), *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  string * int2 = new string();
  int i2 = genPigInt(*int2);
  d2->push_back(int2);
  genPigTuple(*d2, *v2);
  genPigWritable(*v2, *wrt2);

  // order by int asc
  compareAsc<int>(i1, i2, *wrt1, *wrt2, name);
  // order by int desc
  compareDesc<int>(i1, i2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);


  string * int5 = new string(*int2);
  ASSERT_EQ(*int2, *int5);
  string * bool5 = new string();
  genPigBool(*bool5);
  vector<string *> * d5 = new vector<string *>();
  d5->push_back(int5);
  d5->push_back(bool5);

  string * float2 = new string();
  genPigFloat(*float2);
  d2->push_back(float2);

  string * v5 = new string();
  string * wrt5 = new string();
  genPigTuple(*d5, *v5);
  genPigTuple(*d2, *v2);
  genPigWritable(*v5, *wrt5);
  genPigWritable(*v2, *wrt2);

  // (int, float) > (int, bool) => wrt2 > wrt5
  string order = "11";
  compareWithOrder<int>(PigFloatType, PigBoolType, *wrt2, *wrt5, name, order);
  order = "10";
  compareWithOrder<int>(PigBoolType, PigFloatType, *wrt2, *wrt5, name, order);
  compareBytes(*wrt1, *wrt2, name);

  delete d1;
  delete d2;
  delete d5;
  delete int1;
  delete int2;
  delete int5;
  delete v1;
  delete v2;
  delete v5;
  delete wrt1;
  delete wrt2;
  delete wrt5;




}


TEST(NativeObjectFactory, PigTupleComparatorWithBag) {
  config.setBool("native.pig.useSecondaryKey", false);
  config.setBool("native.pig.groupOnly", false);
  config.set("native.pig.sortOrder", "1");
  string name = ".NullableTuple";
  ComparatorPtr comparator = getComparator(name);
  ASSERT_EQ(true, comparator != NULL);

  string * bag1 = new string();
  string * bag2 = new string();

  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();
  vector<string *> * d1 = new vector<string *>();
  vector<string *> * d2 = new vector<string *>();

  string * tInt = new string();
  string * tDouble = new string();
  genPigTupleOne(*tInt, PigIntType);
  genPigTupleOne(*tDouble, PigDoubleType);
  string * tDouble2 = new string(*tDouble);
  ASSERT_EQ(0, PigUtils::compare(*tDouble, *tDouble2));

  d1->push_back(tInt);
  d1->push_back(tDouble);
  d2->push_back(tDouble2);

  genPigBag(*d1, *bag1);
  genPigBag(*d2, *bag2);

  genPigTupleOne(*bag1, *v1);
  genPigTupleOne(*bag2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  ASSERT_GT(d1->size(), d2->size());
  // wrt1.size > wrt2.size => wrt1 > wrt2
  ASSERT_EQ(1, getCompareResult(comparator, *wrt1, *wrt2));

  string *tInt2 = new string(*tInt);
  ASSERT_EQ(0, PigUtils::compare(*tInt, *tInt2));
  d2->push_back(tInt2);
  genPigBag(*d2, *bag2);
  genPigTupleOne(*bag2, *v2);
  genPigWritable(*v2, *wrt2);

  ASSERT_EQ(d1->size(), d2->size());
  // wrt1 == wrt2; the tuples are sorted before comparison
  ASSERT_EQ(0, getCompareResult(comparator, *wrt1, *wrt2));

  string * tNull = new string();
  genPigTupleOne(*tNull, PigNullType);
  d1->push_back(tNull);
  string * tTrue = new string();
  string * fTrue = new string();
  genPigBool(*fTrue, PIG_BOOLEAN_TRUE);
  genPigTupleOne(*fTrue, *tTrue);
  d2->push_back(tTrue);

  genPigBag(*d1, *bag1);
  genPigBag(*d2, *bag2);

  genPigTupleOne(*bag1, *v1);
  genPigTupleOne(*bag2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  ASSERT_EQ(d1->size(), d2->size());
  // {(null), (int), (double)} < {(bool), (int), (double)} => wrt1 < wrt2
  ASSERT_EQ(-1, getCompareResult(comparator, *wrt1, *wrt2));

  string * fFalse = new string();
  string * tFalse = new string();
  genPigBool(*fFalse, PIG_BOOLEAN_FALSE);
  genPigTupleOne(*fFalse, *tFalse);
  d1->push_back(tFalse);
  string * tNull2 = new string(*tNull);
  d2->push_back(tNull2);

  genPigBag(*d1, *bag1);
  genPigBag(*d2, *bag2);
  genPigTupleOne(*bag1, *v1);
  genPigTupleOne(*bag2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  ASSERT_EQ(d1->size(), d2->size());
  // {(null), (false), (int), (double)} < {(null), (true), (int), (double)}
  ASSERT_EQ(-1, getCompareResult(comparator, *wrt1, *wrt2));

  string * tFalse1 = new string(*tFalse);
  string * tFalse2 = new string(*tFalse);
  ASSERT_EQ(*tFalse, *tFalse1);
  ASSERT_EQ(*tFalse, *tFalse2);
  d1->push_back(tFalse1);
  d2->push_back(tFalse2);

  genPigBag(*d1, *bag1);
  genPigBag(*d2, *bag2);
  genPigTupleOne(*bag1, *v1);
  genPigTupleOne(*bag2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  ASSERT_EQ(d1->size(), d2->size());
  // {(null), (false), (false), (int), (double)} < {(null), (false), (true), (int), (double)}
  ASSERT_EQ(-1, getCompareResult(comparator, *wrt1, *wrt2));

  delete tNull;
  delete tNull2;
  delete fFalse;
  delete tFalse;
  delete tFalse1;
  delete tFalse2;
  delete fTrue;
  delete tTrue;
  delete tInt;
  delete tInt2;
  delete tDouble;
  delete tDouble2;
  delete d1;
  delete d2;
  delete bag1;
  delete bag2;
  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}


TEST(NativeObjectFactory, PigBytesComparator) {
  config.setBool("native.pig.useSecondaryKey", false);
  string * v1 = new string();
  string * v2 = new string();
  string * wrt1 = new string();
  string * wrt2 = new string();

  string * f1 = new string();
  string * f2 = new string();
  string * bytes1 = genPigByteArray(*f1, PIG_TINYBYTEARRAY);
  string * bytes2 = genPigByteArray(*f2, PIG_TINYBYTEARRAY);
  genPigTupleOne(*f1, *v1);
  genPigTupleOne(*f2, *v2);
  genPigWritable(*v1, *wrt1);
  genPigWritable(*v2, *wrt2);

  string name = ".NullableBytesWritable";
  compareAsc<string>(*bytes1, *bytes2, *wrt1, *wrt2, name);
  compareDesc<string>(*bytes1, *bytes2, *wrt1, *wrt2, name);
  compareBytes(*wrt1, *wrt2, name);

  delete f1;
  delete f2;
  delete bytes1;
  delete bytes2;
  delete v1;
  delete v2;
  delete wrt1;
  delete wrt2;
}
