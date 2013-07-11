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

#include "commons.h"
#include "lib/combiner.h"
#include "test_commons.h"
#include <iostream>

namespace NativeTask {

class Iterator : public KVIterator {
  std::vector<std::pair<int, int> > * kvs;
  int index;
  char * buffer;

public:
  Iterator() : index(0),
  buffer(NULL){
	buffer= new char[8];
    kvs = new std::vector<std::pair<int, int> >();
    kvs->push_back(std::pair<int, int>(10, 100));

    kvs->push_back(std::pair<int, int>(10, 100));
    kvs->push_back(std::pair<int, int>(10, 101));
    kvs->push_back(std::pair<int, int>(10, 102));

    kvs->push_back(std::pair<int, int>(20, 200));
    kvs->push_back(std::pair<int, int>(20, 201));
    kvs->push_back(std::pair<int, int>(20, 202));
    kvs->push_back(std::pair<int, int>(30, 302));
    kvs->push_back(std::pair<int, int>(40, 302));
  }

  bool next(Buffer & key, Buffer & outValue) {
    if (index < kvs->size()) {
      std::pair<int, int> value = kvs->at(index);
      *((int *)buffer) =  value.first;
      *(((int *)buffer) + 1) =  value.second;
      key.reset(buffer, 4);
      outValue.reset(buffer + 4, 4);
      index++;
      return true;
    }
    return false;
  }
};

void TestKeyGroupIterator() {
  Iterator * iter = new Iterator();
  KeyGroupIteratorImpl * groupIterator = new KeyGroupIteratorImpl(iter);
  const char * key = NULL;
  while(groupIterator->nextKey()) {
    uint32_t length = 0;
    key = groupIterator->getKey(length);
    int * keyPtr = (int *)key;
    std::cout<< "new key: " << *keyPtr << std::endl;
    const char * value = NULL;
    while(NULL != (value = groupIterator->nextValue(length))) {
      int * valuePtr = (int *)value;
      std::cout<< "==== key: " << *keyPtr << "value: " << *valuePtr  << std::endl;
    }
  }
  std::cout <<"Done!!!!!!! "  << std::endl;
}

TEST(Combiner, keyGroupIterator) {
  TestKeyGroupIterator();
}

} /* namespace NativeTask */
