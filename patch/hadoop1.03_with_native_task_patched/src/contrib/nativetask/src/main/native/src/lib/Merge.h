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

#ifndef MERGE_H_
#define MERGE_H_

#include "NativeTask.h"
#include "Buffers.h"
#include "MapOutputCollector.h"
#include "IFile.h"
#include "heap.h"

namespace NativeTask {

/**
 * merger
 */
class MergeEntry {

protected:
  // these 3 fields should be filled after next() is called
  const char *   _key;
  uint32_t _key_len;
  uint32_t _value_len;

public:
  MergeEntry() :
      _key_len(0),
      _value_len(0),
      _key(NULL){
  }

  const char * getKey() const {
    return _key;
  }

  uint32_t getKeyLength() const {
    return _key_len;
  }

  uint32_t getValueLength() const {
    return _value_len;
  }

  virtual ~MergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() = 0;

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() = 0;

  /**
   * read value
   */
  virtual const char * getValue() = 0;
};

/**
 * Merger
 */
typedef MergeEntry * MergeEntryPtr;

class MergeEntryComparator {
private:
  ComparatorPtr _keyComparator;

public:
  MergeEntryComparator(ComparatorPtr comparator) : _keyComparator(comparator) {
  }

public:
  bool operator()(const MergeEntryPtr lhs, const MergeEntryPtr rhs) {
    return (*_keyComparator)(lhs->getKey(), lhs->getKeyLength(), rhs->getKey(), rhs->getKeyLength()) < 0;
  }
};

/**
 * Merge entry for in-memory partition bucket
 */
class MemoryMergeEntry: public MergeEntry {
protected:
  char *  _value;
  MapOutputCollector * _moc;
  PartitionBucket * _pb;
  int64_t _cur_partition;
  int64_t _cur_index;
  const char *  _base;

public:
  MemoryMergeEntry(MapOutputCollector * moc) :
      _moc(moc),
      _pb(NULL),
      _cur_partition(-1ULL),
      _cur_index(-1ULL),
      _value(NULL),
      _base(NULL){
    MemoryBlockPool * pool = _moc->getPool();
    if (NULL != pool) {
      _base = pool->get_base();
    }
  }

  virtual ~MemoryMergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() {
    ++_cur_partition;
    if (_cur_partition < _moc->num_partitions()) {
      _pb = _moc->get_partition_bucket(_cur_partition);
      _cur_index = -1ULL;
      return 0;
    }
    return 1;
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() {
    ++_cur_index;
    if ((NULL != _pb) && (_cur_index < _pb->recored_count())) {
      uint32_t offset = _pb->recored_offset(_cur_index);
      InplaceBuffer * kb = (InplaceBuffer*)(_base + offset);
      _key_len = kb->length;
      _key = kb->content;
      InplaceBuffer & vb = kb->next();
      _value_len = vb.length;
      _value = vb.content;
      assert(_value != NULL);
      return 0;
    }
    // detect error early
    _key_len = 0xffffffff;
    _value_len = 0xffffffff;
    _key = NULL;
    _value = NULL;
    return 1;
  }

  /**
   * read value
   */
  virtual const char * getValue() {
    return _value;
  }
};


/**
 * Merge entry for intermediate file
 */
class IFileMergeEntry : public MergeEntry {
protected:
  IFileReader * _reader;
  bool new_partition;
public:
  /**
   * @param reader: managed by InterFileMergeEntry
   */
  IFileMergeEntry(IFileReader * reader):
    _reader(reader) {
    new_partition = false;
  }

  virtual ~IFileMergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual int nextPartition() {
    return _reader->nextPartition();
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual int next() {
    _key = _reader->nextKey(_key_len);
    if (unlikely(NULL == _key)) {
      // detect error early
      _key_len = 0xffffffffU;
      _value_len = 0xffffffffU;
      return 1;
    }
    _value_len = _reader->valueLen();
    return 0;
  }

  /**
   * read value
   */
  virtual const char * getValue() {
    const char * ret = _reader->value(_value_len);
    assert(ret != NULL);
    return ret;
  }
};

class Merger : public  KVIterator {

private:
  vector<MergeEntryPtr> _entries;
  vector<MergeEntryPtr> _heap;
  IFileWriter * _writer;
  Config & _config;
  ICombineRunner * _combineRunner;
  bool _first;
  MergeEntryComparator _comparator;

public:
  Merger(IFileWriter * writer, Config & config, ComparatorPtr comparator, ICombineRunner * combineRunner=NULL);

  ~Merger();

  void addMergeEntry(MergeEntryPtr pme);

  void merge();

  virtual bool next(Buffer & key, Buffer & value);
protected:
  int startPartition();
  void endPartition();
  void initHeap();
  bool next();
};

} // namespace NativeTask

#endif /* MERGE_H_ */
