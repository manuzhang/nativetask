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

#ifndef RREDUCERHANDLER_H_
#define RREDUCERHANDLER_H_

#include "NativeTask.h"
#include "BatchHandler.h"

namespace NativeTask {

class RReducerHandler : public BatchHandler, public Collector, public KVIterator {

public:
  static const Command RUN;
  static const Command LOAD;

protected:
  ProcessorBase * _processor;

  // RecordWriter
  RecordWriter * _writer;

  // Reduce output collector
  Collector * _collector;

  // counters
  Counter * _inputGroupCounter;
  Counter * _inputRecordCounter;
  Counter * _outputRecordCounter;

  NativeObjectType _reducerType;

  FixSizeContainer _asideBuffer;
  ByteArray _asideBytes;

  char * _inputStart;
  uint32_t _inputLength;
  Endium _endium;

public:
  RReducerHandler();
  virtual ~RReducerHandler();

  virtual void configure(Config * config);
  virtual ResultBuffer * onCall(const Command & cmd, ParameterBuffer * param);

  virtual void handleInput(ByteBuffer & in);

  // Collector methods
  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen);

  virtual bool next(Buffer & key, Buffer & value);

protected:

private:
  void initCounters();
  void run();
  virtual int32_t refill();
  KVBuffer * nextKeyValue(); // return key position

  virtual void writeToJavaRecordWriter(const void * key, uint32_t keyLen, const void * value,
      uint32_t valueLen);

  RecordWriter * getWriter(Config * config);

  KeyGroupIterator * createKeyGroupIterator();

};

} // namespace NativeTask

#endif /* RREDUCERHANDLER_H_ */
