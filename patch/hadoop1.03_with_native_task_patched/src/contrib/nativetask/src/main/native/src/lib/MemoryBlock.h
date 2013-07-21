#include "NativeTask.h"
#include "Timer.h"
#include "Buffers.h"
#include "MapOutputSpec.h"
#include "IFile.h"
#include "SpillInfo.h"
#include "combiner.h"
#include "MapOutputSpec.h"

#ifndef MEMORYBLOCK_H_
#define MEMORYBLOCK_H_

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

}

#endif /* MEMORYBLOCK_H_ */
