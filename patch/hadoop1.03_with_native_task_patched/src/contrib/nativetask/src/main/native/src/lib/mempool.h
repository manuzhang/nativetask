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

#ifndef MEMORYPOOL_H_
#define MEMORYPOOL_H_

#include "Buffers.h"
#include "MapOutputSpec.h"
#include "NativeTask.h"
#include "util/StringUtil.h"

namespace NativeTask {


/**
 * A block of memory used to store small(relatively) Buffers,
 * increase cpu/cache affinity when perform sort, compression, spill
 */
class MemoryBlock {
  friend class MemoryBlockPool;
private:
  uint32_t _size;
  uint32_t _used;
  char * _pos;

public:
  MemoryBlock(uint32_t size, char * pos) :
      _size(size),
      _used(0),
      _pos(pos) {
  }

  uint32_t size() const {
    return _size;
  }

  char * start() const {
    return _pos;
  }

  uint32_t used() const {
    return _used;
  }

  inline char * position() const {
    return _pos + _used;
  }

  inline uint32_t rest() const {
    return _size - _used;
  }

  char * put(void * obj, uint32_t size) {
    assert(_used+size<=_size);
    char * ret = _pos + _used;
    memcpy(ret, obj, size);
    _used += size;
    return ret;
  }

  std::string str() {
    char buff[1024];
    snprintf(buff, 1024, "%016llx: %u/%u", (unsigned long long) _pos, _used, _size);
    return std::string(buff);
  }

};


inline uint32_t GetCeil(uint32_t v, uint32_t unit) {
  return ((v + unit - 1) / unit) * unit;
}

const uint32_t DEFAULT_MIN_BLOCK_SIZE = 16 * 1024;
const uint32_t DEFAULT_MAX_BLOCK_SIZE = 4 * 1024 * 1024;
const uint32_t NULL_BLOCK_INDEX = 0xffffffffU;

/**
 * Class for allocating and manage MemoryBlocks
 */
class MemoryBlockPool {
private:
  bool _inited;
  uint32_t _min_block_size;
  char * _base;
  uint32_t _capacity;
  uint32_t _used;
  std::vector<MemoryBlock> _blocks;

public:
  MemoryBlockPool();

  ~MemoryBlockPool();

  bool init(uint32_t capacity,
      uint32_t min_block_size)
      throw (OutOfMemoryException);

  void reset() {
    _blocks.clear();
    _used = 0;
  }

  bool inited() {
    return _inited;
  }

  MemoryBlock & get_block(uint32_t idx) {
    assert(_blocks.size() > idx);
    return _blocks[idx];
  }

  char * get_position(uint32_t offset) {
    return _base + offset;
  }

  const char * get_base() {
    return _base;
  }

  uint32_t get_offset(void * pos) {
    assert((char*)pos >= _base);
    return (uint32_t) ((char*) pos - _base);
  }

  char * alloc_block(uint32_t & current_block_idx, uint32_t size) {
    uint32_t newsize = GetCeil(size+8, _min_block_size);
    assert(newsize%_min_block_size==0);
    if (size > _capacity) {
      THROW_EXCEPTION_EX(OutOfMemoryException, "size %d larger than io.sort.mb %d", size, _capacity);
    }
    if (_used + size > _capacity) {
      return NULL;
    }
    if (_used + newsize > _capacity) {
      _blocks.push_back(MemoryBlock(_capacity - _used, _base + _used));
      _used = _capacity;
    }
    else {
      _blocks.push_back(MemoryBlock(newsize-8, _base + _used));
      _used += newsize;
    }
    current_block_idx = _blocks.size() - 1;
    MemoryBlock & blk = _blocks[current_block_idx];
    char * ret = blk.position();
    blk._used += size;
    return ret;
  }

  char * allocate_buffer(uint32_t & current_block_idx, uint32_t size) {
    if (likely(current_block_idx != NULL_BLOCK_INDEX)) {
      MemoryBlock & cur_blk = get_block(current_block_idx);
      if (likely(size <= cur_blk.rest())) {
        char * ret = cur_blk.position();
        cur_blk._used += size;
        return ret;
      }
    }
    return alloc_block(current_block_idx, size);
  }

  void dump(FILE * out);
};

} // namespace NativeTask


#endif /* MEMORYPOOL_H_ */
