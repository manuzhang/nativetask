/*
 * MJavaCombineHandler.h
 *
 *  Created on: 13 Jul 2013
 *      Author: xzhong10
 */

#ifndef _COMBINEHANDLER_H_
#define _COMBINEHANDLER_H_

#include "combiner.h"
#include "BatchHandler.h"

namespace NativeTask {

class CombineHandler: public NativeTask::ICombineRunner,
    public NativeTask::BatchHandler {
private:
  CombineContext  * _combineContext;
  KVIterator * _kvIterator;
  IFileWriter * _writer;
  Buffer _key;
  Buffer _value;
  bool _kvCached;
  KeyValueType _kType;
  KeyValueType _vType;
  MapOutputSpec _mapOutputSpec;
  Config * _config;

public:
  CombineHandler();
  virtual ~CombineHandler();

  void handleInput(char * buff, uint32_t length);
  void finish();

  std::string command(const std::string & cmd);

  void configure(Config & config);

  void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

private:
  void delegateToJavaCombiner();
  void flushDataToWriter();
  uint32_t refill(int serializationType);

};

} /* namespace NativeTask */
#endif /* _JAVACOMBINEHANDLER_H_ */
