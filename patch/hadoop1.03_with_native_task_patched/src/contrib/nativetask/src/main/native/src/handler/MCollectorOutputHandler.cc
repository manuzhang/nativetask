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
#include "MCollectorOutputHandler.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"

namespace NativeTask {

using std::string;
using std::vector;

MCollectorOutputHandler::MCollectorOutputHandler() :
    _collector(NULL),
    _dest(NULL) {
}

MCollectorOutputHandler::~MCollectorOutputHandler() {
  reset();
}

void MCollectorOutputHandler::reset() {
  _dest = NULL;
  delete _collector;
  _collector = NULL;
}

void MCollectorOutputHandler::configure(Config & config) {
  uint32_t partition = config.getInt("mapred.reduce.tasks", 1);
  _collector = new MapOutputCollector(partition);
  _collector->configure(config);
}

void MCollectorOutputHandler::finish() {
  string outputpath = this->sendCommand("GetOutputPath");
  string indexpath = this->sendCommand("GetOutputIndexPath");
  if ((outputpath.length() == 0) || (indexpath.length() == 0)) {
    THROW_EXCEPTION(IOException, "Illegal(empty) map output file/index path");
  }
  vector<string> pathes;
  StringUtil::Split(outputpath, ";", pathes);
  _collector->final_merge_and_spill(pathes, indexpath, _collector->get_mapoutput_spec());
  reset();
  BatchHandler::finish();
}

void MCollectorOutputHandler::handleInput(char * buff, uint32_t length) {

  while (length>0) {
    if (unlikely(length<2*sizeof(uint32_t))) {
      THROW_EXCEPTION(IOException, "k/v meta information incomplete");
    }

    // key value format
    // keyLength(4 byte) + key + valueLength(4 byte) + value + partitionId(4 byte)

    //convert to little endium
    char * pos = buff;
    uint32_t keyLength = bswap(*((uint32_t*)pos));
    *((uint32_t*)pos) = keyLength;

    pos += keyLength + sizeof(uint32_t);
    uint32_t valueLength = bswap(*((uint32_t*)pos));
    *((uint32_t*)pos) = valueLength;

    pos += valueLength + sizeof(uint32_t);
    uint32_t partition = bswap(*((uint32_t*)pos));

    uint32_t kvlength = pos - buff;

    char * dest = _collector->get_buffer_to_put(kvlength, partition);
    if (NULL == dest) {
      string spillpath = this->sendCommand("GetSpillPath");
      if (spillpath.length() == 0) {
        THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
      }
      vector<string> pathes;
      StringUtil::Split(spillpath, ";", pathes);
      _collector->middle_spill(pathes, "", _collector->get_mapoutput_spec());
      dest = _collector->get_buffer_to_put(kvlength, partition);
      if (NULL == dest) {
        // io.sort.mb too small, cann't proceed
        // should not get here, cause get_buffer_to_put can throw OOM exception
        THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
      }
    }

    if (unlikely(kvlength > length)) {
      //key and value must be able to flushed together.
	  THROW_EXCEPTION(IOException, "k/v data incomplete");
    }

    simple_memcpy(dest, buff, kvlength);

    //4 bytes for the partition id
    buff += kvlength + sizeof(uint32_t);
    length -= kvlength + sizeof(uint32_t);
  }
}

}
