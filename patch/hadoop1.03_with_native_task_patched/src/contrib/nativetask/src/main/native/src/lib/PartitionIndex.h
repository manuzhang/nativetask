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

#ifndef PARTITIONINDEX_H_
#define PARTITIONINDEX_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * Store spill file segment information
 */
struct IFileSegment {
  // uncompressed stream end position
  uint64_t uncompressedEndOffset;
  // compressed stream end position
  uint64_t realEndOffset;
};

class SpillInfo {
public:
  uint32_t start;
  uint32_t length;
  std::string path;
  IFileSegment * segments;

  SpillInfo(IFileSegment * segments, uint32_t len, const string & path) :
    start(start), length(len), path(path), segments(segments) {
  }

  ~SpillInfo() {
    delete [] segments;
  }

  void delete_file();

  uint64_t getEndPosition() {
    return segments ? segments[length-1].uncompressedEndOffset : 0;
  }

  uint64_t getRealEndPosition() {
    return segments ? segments[length-1].realEndOffset : 0;
  }
};

class PartitionIndex {
protected:
  // TODO: fix this field
  uint32_t _num_partition;
public:
  std::vector<SpillInfo*> ranges;
  PartitionIndex(uint32_t num_partition) :
    _num_partition(num_partition) {
  }

  ~PartitionIndex() {
    for (size_t i = 0; i < ranges.size(); i++) {
      delete ranges[i];
    }
    ranges.clear();
  }

  void deleteFiles() {
    for (size_t i = 0; i < ranges.size(); i++) {
      ranges[i]->delete_file();
    }
  }

  void add(SpillInfo * sri) {
    ranges.push_back(sri);
  }

  void writeIFile(const std::string & filepath);
};


} // namespace NativeTask


#endif /* PARTITIONINDEX_H_ */
