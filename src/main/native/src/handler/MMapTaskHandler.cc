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
#include "MMapTaskHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace NativeTask {

const Command MMapTaskHandler::RUN(3, "RUN");

MMapTaskHandler::MMapTaskHandler()
    : _numPartition(1), _config(NULL), _reader(NULL), _mapper(NULL), _partitioner(NULL), _moc(NULL),
        _writer(NULL), _mapInputRecords(NULL), _mapInputBytes(NULL), _mapOutputRecords(NULL),
        _mapOutputBytes(NULL) {
}

MMapTaskHandler::~MMapTaskHandler() {
  delete _reader;
  _reader = NULL;
  delete _mapper;
  _mapper = NULL;
  delete _partitioner;
  _partitioner = NULL;
  delete _moc;
  _moc = NULL;
  delete _writer;
  _writer = NULL;
}

void MMapTaskHandler::initCounters() {
  _mapInputRecords = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_INPUT_RECORDS);
  _mapInputBytes = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_INPUT_BYTES);
  _mapOutputRecords = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_OUTPUT_RECORDS);
  _mapOutputBytes = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::MAP_OUTPUT_BYTES);
}

void MMapTaskHandler::configure(Config * config) {
  initCounters();

  _config = config;
  _numPartition = config->getInt(MAPRED_NUM_REDUCES, 1);

  const char * readerClass = config->get(NATIVE_RECORDREADER);
  if (NULL == readerClass) {
    THROW_EXCEPTION(IOException, "native.recordreader.class not set");
  }
  _reader = (RecordReader*)NativeObjectFactory::CreateObject(readerClass);
  if (NULL == _reader) {
    THROW_EXCEPTION_EX(UnsupportException, "%s not found", readerClass);
  }
  _reader->configure(config);

  if (_numPartition > 0) {

    // collector
    _moc = new MapOutputCollector(_numPartition, this);
    _moc->configure(config);

    // partitioner
    const char * partitionerClass = config->get(NATIVE_PARTITIONER);
    if (NULL != partitionerClass) {
      _partitioner = (Partitioner *)NativeObjectFactory::CreateObject(partitionerClass);
    } else {
      _partitioner = (Partitioner *)NativeObjectFactory::CreateDefaultObject(PartitionerType);
    }
    if (NULL == _partitioner) {
      THROW_EXCEPTION(IOException, "Partitioner not found");
    }
    _partitioner->configure(config);

    LOG("[MMapTaskHandler] Native Mapper with native MapOutputCollector, RecordReader: %s Partitioner: %s",
        readerClass ? readerClass : "Java RecordReader",
        partitionerClass ? partitionerClass : "default");
  } else {
    const char * writerClass = config->get(NATIVE_RECORDWRITER);
    if (NULL == writerClass) {
      THROW_EXCEPTION(IOException, "RecordWriter not found");
    }
    _writer = (RecordWriter*)NativeObjectFactory::CreateObject(writerClass);
    _writer->configure(config);
    LOG("[MMapTaskHandler] Native Mapper with RecordReader: %s RecordWriter: %s",
        readerClass ? readerClass : "Java RecordReader", writerClass);
  }

  // mapper
  const char * mapperClass = config->get(NATIVE_MAPPER);
  if (NULL != mapperClass) {
    _mapper = (Mapper *)NativeObjectFactory::CreateObject(mapperClass);
  } else {
    _mapper = (Mapper *)NativeObjectFactory::CreateDefaultObject(MapperType);
  }
  if (NULL == _mapper) {
    THROW_EXCEPTION(UnsupportException, "Mapper not found");
  }
  _mapper->setCollector(this);
  _mapper->configure(config);
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen, int partition) {
  _mapOutputRecords->increase();
  _mapOutputBytes->increase(keyLen + valueLen);
  if (NULL != _moc) {
    _moc->collect(key, keyLen, value, valueLen, partition);
  }
}

void MMapTaskHandler::collect(const void * key, uint32_t keyLen, const void * value,
    uint32_t valueLen) {
  if (NULL != _moc) {
    uint32_t partition = _partitioner->getPartition((const char *)key, keyLen, _numPartition);
    collect(key, keyLen, value, valueLen, partition);
  } else {
    _mapOutputRecords->increase();
    _mapOutputBytes->increase(keyLen + valueLen);
    _writer->collect(key, keyLen, value, valueLen);
  }
}

ResultBuffer * MMapTaskHandler::onCall(const Command& command, ParameterBuffer * param) {
  if (!RUN.equals(command)) {
    THROW_EXCEPTION_EX(UnsupportException, "Command Not supported, %d", command.id());
  }
  if (_reader == NULL || _mapper == NULL) {
    THROW_EXCEPTION(IOException, "MMapTaskHandler not setup yet");
  }
  NativeObjectFactory::SetTaskProgressSource(_reader);
  Buffer key;
  Buffer value;
  while (_reader->next(key, value)) {
    _mapInputRecords->increase();
    _mapInputBytes->increase(key.length() + value.length());
    _mapper->map(key.data(), key.length(), value.data(), value.length());
  }

  _mapper->close();
  if (NULL != _moc) {
    _moc->close();
  } else {
    _writer->close();
  }

  NativeObjectFactory::SetTaskProgressSource(NULL);
  return NULL;
}

} // namespace NativeTask

