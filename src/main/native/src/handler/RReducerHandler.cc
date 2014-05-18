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
#include "util/StringUtil.h"
#include "TaskCounters.h"
#include "NativeObjectFactory.h"
#include "RReducerHandler.h"
#include <iostream>

namespace NativeTask {

const Command RReducerHandler::RUN(3, "RUN");
const Command RReducerHandler::LOAD(1, "Load");

RReducerHandler::RReducerHandler()
    : _processor(NULL), _writer(NULL), _collector(NULL), _inputGroupCounter(NULL),
        _inputRecordCounter(NULL), _outputRecordCounter(NULL), _reducerType(UnknownObjectType),
        _inputStart(NULL), _inputLength(0), _endium(LARGE_ENDIUM) {
}

RReducerHandler::~RReducerHandler() {
  if (NULL != _processor) {
    delete _processor;
    _processor = NULL;
  }

  if (NULL != _collector) {
    delete _collector;
    _collector = NULL;
  }

  if (NULL != _writer) {
    delete _writer;
    _writer = NULL;
  }
}

void RReducerHandler::initCounters() {
  _inputRecordCounter = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::REDUCE_INPUT_RECORDS);
  _outputRecordCounter = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::REDUCE_OUTPUT_RECORDS);
  _inputGroupCounter = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::REDUCE_INPUT_GROUPS);
}

void RReducerHandler::handleInput(ByteBuffer & in) {
  if (_asideBuffer.size() > 0) {
    if (_asideBuffer.remain() > 0) {
      uint32_t filledLength = _asideBuffer.fill(in.current(), in.remain());
      in.advance(filledLength);
    }

    if (_asideBuffer.remain() == 0) {
      _asideBuffer.position(0);
      return;
    }
  }

  if (in.remain() <= 0) {
    return;
  }

  KVBuffer * kvBuffer = (KVBuffer *)(in.current());
  uint32_t kvLength = kvBuffer->lengthConvertEndium();

  if (kvLength > in.remain()) {
    _asideBytes.resize(kvLength);
    _asideBuffer.wrap(_asideBytes.buff(), _asideBytes.size());
    uint32_t filledLength = _asideBuffer.fill(in.current(), in.remain());
    in.advance(filledLength);
    return;
  }
}

RecordWriter * RReducerHandler::getWriter(Config * config) {
  RecordWriter * writer = NULL;
  const char * writerClass = config->get(NATIVE_RECORDWRITER);
  if (NULL != writerClass) {
    writer = (RecordWriter*)NativeObjectFactory::CreateObject(writerClass);
    if (NULL == writer) {
      THROW_EXCEPTION_EX(IOException, "native.recordwriter.class %s not found", writerClass);
    }
    writer->configure(config);
  }
  return writer;
}

void RReducerHandler::configure(Config * config) {
  initCounters();

  _writer = getWriter(config);
  if (NULL == _writer) {
    //Use Java Collector,
    _collector = new TrackingCollector(this, _outputRecordCounter);
  } else {
    _collector = new TrackingCollector(_writer, _outputRecordCounter);
  }

  const char * reducerClass = config->get(NATIVE_REDUCER);
  if (NULL != reducerClass) {
    NativeObject * obj = NativeObjectFactory::CreateObject(reducerClass);
    if (NULL == obj) {
      THROW_EXCEPTION_EX(IOException, "native.reducer.class %s not found", reducerClass);
    }
    _reducerType = obj->type();
  } else {
    _reducerType = MapperType;
    _processor = (ProcessorBase *)NativeObjectFactory::CreateDefaultObject(MapperType);
  }

  switch (_reducerType) {
  case ReducerType:
  case MapperType:
  case FolderType:
    _processor->setCollector(_collector);
    _processor->configure(config);
    break;
  default:
    THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
  }
}

ResultBuffer * RReducerHandler::onCall(const Command & cmd, ParameterBuffer * param) {
  if (!RUN.equals(cmd)) {
    THROW_EXCEPTION_EX(UnsupportException, "Command Not supported, %d", cmd.id());
  }
  run();
  return NULL;
}

void RReducerHandler::run() {
  switch (_reducerType) {
  case ReducerType: {
    Reducer * reducer = (Reducer *)_processor;
    KeyGroupIterator * iter = createKeyGroupIterator();
    while (iter->nextKey()) {
      _inputGroupCounter->increase();
      reducer->reduce(*iter);
    }
    reducer->close();
    delete iter;
  }
    break;
  case MapperType: {
    Mapper * mapper = (Mapper *)_processor;
    KVBuffer * buffer = NULL;
    while (NULL != (buffer = nextKeyValue())) {
      mapper->map(buffer->getKey(), buffer->keyLength, buffer->getValue(), buffer->valueLength);
    }
    mapper->close();
  }
    break;
  case FolderType:
    THROW_EXCEPTION(UnsupportException, "Folder API not supported");
    break;
  default:
    THROW_EXCEPTION(UnsupportException, "Reducer type not supported");
  }
  if (_writer != NULL) {
    _writer->close();
  } else {
    finish();
  }
}

/**
 * Java record writer
 */
void RReducerHandler::collect(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen) {
  writeToJavaRecordWriter(key, keyLen, value, valueLen);
}

void RReducerHandler::writeToJavaRecordWriter(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen) {
  // pre-mature flush so that we can avoid the cross-block Key-Value.
  // Cross-block Key-Value will need extra memory copy.
  uint32_t kvLength = keyLen + valueLen + 2 * sizeof(uint32_t);
  if (kvLength > _out.remain()) {
    flushOutput();
  }

  outputInt(bswap(keyLen));
  outputInt(bswap(valueLen));
  output((char *)key, keyLen);
  output((char *)value, valueLen);
}

int32_t RReducerHandler::refill() {

  int32_t expectedLength = _in.capacity();

  ParameterBuffer * param = new ParameterBuffer(4);
  param->writeInt(expectedLength);
  ResultBuffer * result = call(LOAD, param);
  uint32_t retvalue = result->readInt();

  delete param;
  delete result;

  return retvalue;
}

bool RReducerHandler::next(Buffer & key, Buffer & value) {
  KVBuffer * kv = nextKeyValue();
  if (NULL == kv) {
    return false;
  }
  key.reset(kv->getKey(), kv->keyLength);
  value.reset(kv->getValue(), kv->valueLength);
  return true;
}

KVBuffer * RReducerHandler::nextKeyValue() {

  if (_asideBuffer.remain() == 0 && _in.remain() == 0) {
    refill();
  }

  if (_asideBuffer.remain() > 0) {
    KVBuffer * kv = (KVBuffer *)(_asideBuffer.current());
    kv->keyLength = bswap(kv->keyLength);
    kv->valueLength = bswap(kv->valueLength);
    _asideBuffer.position(_asideBuffer.size());
    return kv;
  } else if (_in.remain() > 0) {
    KVBuffer * kv = (KVBuffer *)(_in.current());
    kv->keyLength = bswap(kv->keyLength);
    kv->valueLength = bswap(kv->valueLength);
    _in.advance(kv->length());
    return kv;
  } else {
    return NULL;
  }
}

KeyGroupIterator * RReducerHandler::createKeyGroupIterator() {
  return new KeyGroupIteratorImpl(this);
}

} // namespace NativeTask

