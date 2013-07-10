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
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "Merge.h"

namespace NativeTask {

Merger::Merger(IFileWriter * writer, Config & config, ComparatorPtr comparator, ObjectCreatorFunc combinerCreator) :
    _writer(writer),
    _config(config),
    _combinerCreator(combinerCreator),
    _first(true),
    _keyGroupIterState(NEW_KEY),
    _comparator(comparator){
}

Merger::~Merger() {
  _heap.clear();
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    delete _entries[i];
  }
  _entries.clear();
}

void Merger::addMergeEntry(MergeEntryPtr pme) {
  _entries.push_back(pme);
}

/**
 * 0 if success, have next partition
 * 1 if failed, no more
 */
int Merger::startPartition() {
  int ret = -1;
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    int r = _entries[i]->nextPartition();
    if (ret == -1) {
      ret = r;
    } else if (r != ret) {
      THROW_EXCEPTION(IOException, "MergeEntry partition number not equal");
    }
  }
  if (0==ret) { // do have new partition
    _writer->startPartition();
  }
  return ret;
}

/**
 * finish one partition
 */
void Merger::endPartition() {
  _writer->endPartition();
}

void Merger::initHeap() {
  _heap.clear();
  for (size_t i = 0 ; i < _entries.size() ; i++) {
    MergeEntryPtr pme = _entries[i];
    if (0==pme->next()) {
      _heap.push_back(pme);
    }
  }
  make_heap(&(_heap[0]), &(_heap[0])+_heap.size(), _comparator);
}

bool Merger::next() {
  size_t cur_heap_size = _heap.size();
  if (cur_heap_size > 0) {
    if (!_first) {
      if (0 == _heap[0]->next()) { // have more, adjust heap
        if (cur_heap_size == 1) {
          return true;
        } else if (cur_heap_size == 2) {
          MergeEntryPtr * base = &(_heap[0]);

          if (_comparator(base[1], base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          MergeEntryPtr * base = &(_heap[0]);
          adjust_heap(base, 1, cur_heap_size, _comparator);
        }
      } else { // no more, pop heap
        MergeEntryPtr * base = &(_heap[0]);
        pop_heap(base, base+cur_heap_size, _comparator);
        _heap.pop_back();
      }
    } else {
      _first = false;
    }
    return _heap.size()>0;
  }
  return false;
}

bool Merger::nextKey() {
  if (_keyGroupIterState == NO_MORE) {
    return false;
  }

  uint32_t temp;
  while (_keyGroupIterState == SAME_KEY ||
      _keyGroupIterState == NEW_KEY_VALUE) {
    nextValue(temp);
  }
  if (_keyGroupIterState ==  NEW_KEY) {
    if (unlikely(_first == true)) {
      if (!next()) {
        _keyGroupIterState = NO_MORE;
        return false;
      }
    }
    _currentGroupKey.assign(_heap[0]->getKey(), _heap[0]->getKeyLength());
    _keyGroupIterState = NEW_KEY_VALUE;
    return true;
  }
  return false;
}

const char * Merger::getKey(uint32_t & len) {
  len = (uint32_t)_currentGroupKey.length();
  return _currentGroupKey.c_str();
}

const char * Merger::nextValue(uint32_t & len) {
  char * pos;
  switch (_keyGroupIterState) {
  case NEW_KEY: {
    return NULL;
  }
  case SAME_KEY: {
    if (next()) {
      if (_heap[0]->getKeyLength() == _currentGroupKey.length()) {
        if (fmemeq(_heap[0]->getKey(), _currentGroupKey.c_str(), _heap[0]->getKeyLength())) {
          len = _heap[0]->getValueLength();
          return _heap[0]->getValue();
        }
      }
      _keyGroupIterState = NEW_KEY;
      return NULL;
    }
    _keyGroupIterState = NO_MORE;
    return NULL;
  }
  case NEW_KEY_VALUE: {
    _keyGroupIterState = SAME_KEY;
    len = _heap[0]->getValueLength();
    return _heap[0]->getValue();
  }
  case NO_MORE:
    return NULL;
  }
  return NULL;
}

void Merger::merge() {
  Timer timer;
  uint64_t total_record = 0;
  uint64_t keyGroupCount = 0;
  _heap.reserve(_entries.size());
  MergeEntryPtr * base = &(_heap[0]);
  while (0 == startPartition()) {
    initHeap();
    if (_heap.size() == 0) {
      endPartition();
      continue;
    }
    _first = true;
    if (_combinerCreator == NULL) {
      while (next()) {
        _writer->writeKey(base[0]->getKey(), base[0]->getKeyLength(), base[0]->getValueLength());
        _writer->writeValue(base[0]->getValue(), base[0]->getValueLength());
        total_record++;
      }
    } else {
      NativeObject * combiner = _combinerCreator();
      if (combiner == NULL) {
        THROW_EXCEPTION_EX(UnsupportException, "Create combiner failed");
      }
      switch (combiner->type()) {
      case MapperType:
        {
          Mapper * mapper = (Mapper*)combiner;
          mapper->setCollector(_writer);
          mapper->configure(_config);
          while (next()) {
            mapper->map(base[0]->getKey(), base[0]->getKeyLength(), base[0]->getValue(), base[0]->getValueLength());
          }
          mapper->close();
          delete mapper;
        }
        break;
      case ReducerType:
        {
          _keyGroupIterState = NEW_KEY;
          Reducer * reducer = (Reducer*)combiner;
          reducer->setCollector(_writer);
          reducer->configure(_config);
          while (nextKey()) {
            keyGroupCount++;
            reducer->reduce(*this);
          }
          reducer->close();
          delete reducer;
        }
        break;
      default:
        delete combiner;
        THROW_EXCEPTION(UnsupportException, "Combiner type not support");
      }
    }
    endPartition();
  }
  double interval = (timer.now() - timer.last())/1000000000.0;
  uint64_t output_size;
  uint64_t real_output_size;
  _writer->getStatistics(output_size, real_output_size);
  if (keyGroupCount == 0) {
    LOG("[Merge] Merged segment#: %lu, record#: %llu, avg record size: %.3lf, uncompressed total bytes: %llu, compressed total bytes: %llu, time: %.3lfs",
        _entries.size(),
        total_record,
        (double)output_size/total_record,
        output_size,
        real_output_size,
        interval);
  } else {
    LOG("[Merge] Merged segments#, %lu, record#: %llu, key count: %llu, uncompressed total bytes: %llu, compressed total bytes: %llu, time: %.3lfs",
        _entries.size(),
        total_record,
        keyGroupCount,
        output_size,
        real_output_size,
        interval);
  }
}

} // namespace NativeTask
