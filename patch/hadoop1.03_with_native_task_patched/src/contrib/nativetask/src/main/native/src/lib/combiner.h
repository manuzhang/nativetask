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
};

class ICombineRunner {
public:
  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer) = 0;

protected:
  virtual ~ICombineRunner() {
  }
};

class CombineRunner : public ICombineRunner {
private:
  Configurable * _combiner;
  uint32_t _keyGroupCount;
  NativeObjectType _type;

public:
  CombineRunner(Configurable * combiner);

public:
  void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

private:
  KeyGroupIterator * createKeyGroupIterator(KVIterator * iter);
};

} /* namespace NativeTask */
#endif /* COMBINER_H_ */
