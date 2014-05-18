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

#include "commons.h"
#include "util/StringUtil.h"
#include "MMapperHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace NativeTask {

MMapperHandler::MMapperHandler()
    : _config(NULL), _moc(NULL), _mapper(NULL), _partitioner(NULL), _numPartition(1),
        _endium(LARGE_ENDIUM) {
}

MMapperHandler::~MMapperHandler() {

  delete _mapper;
  _mapper = NULL;

  delete _moc;
  _moc = NULL;

  delete _partitioner;
  _partitioner = NULL;
}

void MMapperHandler::configure(Config * config) {
  if (NULL == config) {
    return;
  }
  _config = config;

  // collector
  _numPartition = config->getInt(MAPRED_NUM_REDUCES, 1);
  if (_numPartition > 0) {

    // partitioner
    const char * partitionerClass = config->get(NATIVE_PARTITIONER);
    if (NULL != partitionerClass) {
      _partitioner = (Partitioner *)NativeObjectFactory::CreateObject(partitionerClass);
    } else {
      _partitioner = (Partitioner *)NativeObjectFactory::CreateDefaultObject(PartitionerType);
    }
    if (NULL == _partitioner) {
      THROW_EXCEPTION_EX(UnsupportException, "Partitioner not found: %s", partitionerClass);
    }
    _partitioner->configure(config);

    LOG("[MMapperHandler] Native MapOutputCollector enabled");

    _moc = new MapOutputCollector(_numPartition, this);
    _moc->configure(config);
  } else {
    LOG("[MMapperHandler] Java output collector enabled");
  }

  // mapper
  const char * mapperClass = config->get(NATIVE_MAPPER);
  if (NULL != mapperClass) {
    _mapper = (Mapper *)NativeObjectFactory::CreateObject(mapperClass);
  } else {
    _mapper = (Mapper *)NativeObjectFactory::CreateDefaultObject(MapperType);
  }
  if (NULL == _mapper) {
    THROW_EXCEPTION_EX(UnsupportException, "Mapper not found: %s", mapperClass);
  }
  _mapper->configure(config);
  _mapper->setCollector(this);
}

void MMapperHandler::finish() {

  _mapper->close();
  if (NULL != _moc) {
    _moc->close();
  }

  BatchHandler::finish();
}

void MMapperHandler::map(char * buf, uint32_t length) {
  KVBuffer * kv = NULL;
  char * pos = buf;
  uint32_t remain = length;
  while (remain > 0) {
    kv = (KVBuffer *)pos;
    kv->keyLength = bswap(kv->keyLength);
    kv->valueLength = bswap(kv->valueLength);

    _mapper->map(kv->content, kv->keyLength, kv->getValue(), kv->valueLength);
    remain -= kv->length();
    pos += kv->length();
    ;
  }
}

void MMapperHandler::handleInput(ByteBuffer & in) {
  char * buff = in.current();
  uint32_t length = in.remain();
  const char * end = buff + length;
  uint32_t remain = length;
  char * pos = buff;
  if (_asideBuffer.remain() > 0) {
    uint32_t filledLength = _asideBuffer.fill(pos, length);
    pos += filledLength;
    remain -= filledLength;
  }

  if (_asideBuffer.size() > 0 && _asideBuffer.remain() == 0) {
    _asideBuffer.position(0);
    map(_asideBuffer.current(), _asideBuffer.size());
    _asideBuffer.wrap(NULL, 0);
  }

  if (remain == 0) {
    return;
  }
  KVBuffer * kvBuffer = (KVBuffer *)pos;

  if (unlikely(remain < kvBuffer->headerLength())) {
    THROW_EXCEPTION(IOException, "k/v meta information incomplete");
  }

  int kvLength = kvBuffer->lengthConvertEndium();

  if (kvLength > remain) {
    _asideBytes.resize(kvLength);
    _asideBuffer.wrap(_asideBytes.buff(), _asideBytes.size());
    _asideBuffer.fill(pos, remain);
    pos += remain;
    remain = 0;
  } else {
    map(pos, remain);
  }
}

void MMapperHandler::collect(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen, int partition) {
  if (NULL != _moc) {
    _moc->collect(key, keyLen, value, valueLen, partition);
  }
}

void MMapperHandler::collectInJavaNoReducer(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen) {

  // pre-mature flush so that we can avoid the cross-block Key-Value.
  // Cross-block Key-Value will need extra memory copy.
  uint32_t kvLength = keyLen + valueLen + KVBuffer::headerLength();
  if (0 != _out.position() && kvLength > _out.remain()) {
    flushOutput();
  }

  outputInt(bswap(keyLen));
  outputInt(bswap(valueLen));
  output((char *)key, keyLen);
  output((char *)value, valueLen);

  return;
}

void MMapperHandler::collect(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen) {
  if (NULL == _moc) {
    collectInJavaNoReducer(key, keyLen, value, valueLen);
    return;
  }
  uint32_t partition = _partitioner->getPartition((const char *)key, keyLen, _numPartition);
  collect(key, keyLen, value, valueLen, partition);
}

} // namespace NativeTask
