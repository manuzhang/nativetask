#ifndef COMBINER_H_
#define COMBINER_H_
#include "commons.h"
#include "NativeTask.h"
#include "IFile.h"

namespace NativeTask {

class MemoryBufferKVIterator : public KVIterator{
public:
  virtual const char * getBase() = 0;
  virtual std::vector<uint32_t> * getKVOffsets() = 0;
};

enum CombineContextType {
  UNKNOWN = 0,
  CONTINUOUS_MEMORY_BUFFER = 1,
};

class CombineContext {

private:
  CombineContextType _type;

public:
  CombineContext(CombineContextType type) : _type(type) {
  }

public:
  CombineContextType getType() {
    return _type;
  }
};

class CombineInMemory : public CombineContext {
  CombineInMemory() : CombineContext(CONTINUOUS_MEMORY_BUFFER) {
  }
};

class ICombineRunner {
public:
  ICombineRunner() {
  }

  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) = 0;

protected:
  virtual ~ICombineRunner() {
  }
};

class KeyGroupIteratorImpl : public KeyGroupIterator {
protected:
  // for KeyGroupIterator
  KeyGroupIterState _keyGroupIterState;
  KVIterator * _iterator;
  string _currentGroupKey;
  Buffer _key;
  Buffer _value;
  bool _first;

public:
  KeyGroupIteratorImpl(KVIterator * iterator);
  bool nextKey();
  const char * getKey(uint32_t & len);
  const char * nextValue(uint32_t & len);

protected:
  bool next();
};


class CombineRunner : public ICombineRunner {
private:
  Config * _config;
  ObjectCreatorFunc _combinerCreator;
  uint32_t _keyGroupCount;

public:
  CombineRunner(Config * config, ObjectCreatorFunc objectCreator);

public:
  void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

private:
  KeyGroupIterator * createKeyGroupIterator(KVIterator * iter);
};

} /* namespace NativeTask */
#endif /* COMBINER_H_ */
