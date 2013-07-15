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
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "FileSystem.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"
#include "Merge.h"
#include "NativeTask.h"
#include "WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "combiner.h"
#include "TaskCounters.h"

namespace NativeTask {


/////////////////////////////////////////////////////////////////
// PartitionBucket
/////////////////////////////////////////////////////////////////

uint64_t PartitionBucket::estimate_spill_size(OutputFileType output_type,
    KeyValueType ktype, KeyValueType vtype) {
  int64_t ret = 0;
  for (size_t i = 0; i<_blk_ids.size() ; i++) {
    MemoryBlock & blk = MemoryBlockPool::get_block(_blk_ids[i]);
    ret += blk.used();
  }
  if (output_type == INTERMEDIATE) {
    return ret+sizeof(uint32_t);
  }
  int64_t average_kv_size =
      (ret - (_kv_offsets.size() * sizeof(uint32_t) * 2)) / (_kv_offsets.size()*2);
  int64_t vhead_len = WritableUtils::GetVLongSize(average_kv_size);
  if (ktype == TextType) {
    ret += _kv_offsets.size() * 2 * (2*vhead_len - sizeof(uint32_t));
  }
  else if (ktype == BytesType) {
    ret += _kv_offsets.size() * 2* vhead_len;
  }
  else {
    ret += _kv_offsets.size() * 2 * vhead_len;
  }
  return ret+2+sizeof(uint32_t);
}

bool PartitionBucket::Iterator::next(Buffer & key, Buffer & value) {
  if (index<pb._kv_offsets.size()) {
    KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(pb._kv_offsets[index]);
    InplaceBuffer & bkey = pkvbuffer->get_key();
    InplaceBuffer & bvalue = pkvbuffer->get_value();
    key.reset(bkey.content, bkey.length);
    value.reset(bvalue.content, bvalue.length);
    ++index;
    return true;
  }
  return false;
}

class DirectMemoryBufferedKVIterator : public MemoryBufferKVIterator {
private:
  std::vector<uint32_t> & _kvOffsets;
  uint32_t _index;
  uint32_t _recordSize;
  const char * _base;

public:
  DirectMemoryBufferedKVIterator(const char * base, std::vector<uint32_t> & kvOffsets):
    _base(base), _kvOffsets(kvOffsets), _index(0), _recordSize(kvOffsets.size()){
  }

  const char * getBase() {
    return _base;
  }

  std::vector<uint32_t> * getKVOffsets() {
    return &_kvOffsets;
  }

  bool next(Buffer & key, Buffer & value) {
    if (_index < _recordSize) {
      KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kvOffsets[_index]);
      InplaceBuffer & bkey = pkvbuffer->get_key();
      InplaceBuffer & bvalue = pkvbuffer->get_value();

      key.reset(bkey.content, bkey.length);
      value.reset(bvalue.content, bvalue.length);
      _index++;
      return true;
    }
    return false;
  }
};

void PartitionBucket::spill(IFileWriter & writer, uint64_t & keyGroupCount)
    throw (IOException, UnsupportException) {

  if (_kv_offsets.size() == 0) {
    return;
  }
  if (_combineRunner == NULL) {
    LOG("[PartitionBucket::spill] combiner is not enabled");
    for (size_t i = 0; i<_kv_offsets.size() ; i++) {
      KVBuffer * pkvbuffer = (KVBuffer*)MemoryBlockPool::get_position(_kv_offsets[i]);
      InplaceBuffer & bkey = pkvbuffer->get_key();
      InplaceBuffer & bvalue = pkvbuffer->get_value();
      writer.write(bkey.content, bkey.length, bvalue.content, bvalue.length);
    }
  } else {
    LOG("[PartitionBucket::spill] combiner is enabled");
    MemoryBufferKVIterator * kvIterator = new DirectMemoryBufferedKVIterator(MemoryBlockPool::get_position(0),
        _kv_offsets);

    _combineRunner->combine(CombineContext(CONTINUOUS_MEMORY_BUFFER), kvIterator, &writer);
    delete kvIterator;
  }
}

class ComparatorForDualPivotSort {
private:
  ComparatorPtr _keyComparator;
public:
  ComparatorForDualPivotSort(ComparatorPtr comparator) : _keyComparator(comparator){
  }

  inline int operator()(uint32_t lhs, uint32_t rhs) {
      InplaceBuffer * lhb = (InplaceBuffer*) MemoryBlockPool::get_position(lhs);
      InplaceBuffer * rhb = (InplaceBuffer*) MemoryBlockPool::get_position(rhs);
      return (*_keyComparator)(lhb->content, lhb->length, rhb->content, rhb->length);
  }
};

class ComparatorForStdSort {
private:
  ComparatorPtr _keyComparator;
public:
  ComparatorForStdSort(ComparatorPtr comparator) : _keyComparator(comparator){
  }

public:
  inline bool operator()(uint32_t lhs, uint32_t rhs) {
      InplaceBuffer * lhb = (InplaceBuffer*) MemoryBlockPool::get_position(lhs);
      InplaceBuffer * rhb = (InplaceBuffer*) MemoryBlockPool::get_position(rhs);
      int ret =  (*_keyComparator)(lhb->content, lhb->length, rhb->content,  rhb->length);
      return ret < 0;
  }
};

void PartitionBucket::sort(SortAlgorithm type) {
  if ((!_sorted) && (_kv_offsets.size()>1)) {
    switch (type) {
    case CPPSORT:
      std::sort(_kv_offsets.begin(), _kv_offsets.end(), ComparatorForStdSort(_keyComparator));
      break;
    case DUALPIVOTSORT:
      DualPivotQuicksort(_kv_offsets, ComparatorForDualPivotSort(_keyComparator));
      break;
    default:
      THROW_EXCEPTION(UnsupportException, "Sort Algorithm not support");
    }
  }
  _sorted = true;
}

void PartitionBucket::dump(int fd, uint64_t offset, uint32_t & crc) {
  FILE * out = fdopen(fd, "w");
  fprintf(out, "Partition %d total %lu kv pairs, sorted: %s\n", _partition,
          _kv_offsets.size(), _sorted?"true":"false");
  for (size_t i = 0; i < _kv_offsets.size(); i++) {
    KVBuffer * kv = (KVBuffer*) MemoryBlockPool::get_position(_kv_offsets[i]);
    std::string info = kv->str();
    fwrite(info.c_str(), 1, info.length(), out);
    fputc('\n', out);
  }
  fputc('\n', out);
}


/////////////////////////////////////////////////////////////////
// MapOutputCollector
/////////////////////////////////////////////////////////////////

MapOutputCollector::MapOutputCollector(uint32_t num_partition, ICombineRunner * combineHandler) :
  _config(NULL),
  _buckets(NULL),
  _keyComparator(NULL),
  _combineRunner(combineHandler),
  _spilledRecords(NULL){
  _num_partition = num_partition;
  _buckets = new PartitionBucket*[num_partition];
  memset(_buckets, 0, sizeof(PartitionBucket*) * num_partition);
}

MapOutputCollector::~MapOutputCollector() {
  if (_buckets) {
    for (uint32_t i = 0; i < _num_partition; i++) {
      delete _buckets[i];
    }
  }
  delete[] _buckets;
  MemoryBlockPool::release();
}

void MapOutputCollector::delete_temp_spill_files() {
  _spillInfo.deleteAllSpillFiles();
}

void MapOutputCollector::init(uint32_t memory_capacity, ComparatorPtr keyComparator) {
  if (!MemoryBlockPool::inited()) {
    // At least DEFAULT_MIN_BLOCK_SIZE
    // TODO: at most  DEFUALT_MAX_BLOCK_SIZE
    // and make every bucket have approximately 4 blocks
    uint32_t s = memory_capacity / _num_partition / 4;
    s = GetCeil(s, DEFAULT_MIN_BLOCK_SIZE);
    s = std::max(s, DEFAULT_MIN_BLOCK_SIZE);
    s = std::min(s, DEFAULT_MAX_BLOCK_SIZE);
    //TODO: add support for customized comparator
    this->_keyComparator = keyComparator;
    MemoryBlockPool::init(memory_capacity, s);
  }
  _spilledRecords = NativeObjectFactory::GetCounter(
      TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::SPILLED_RECORDS);
}

void MapOutputCollector::reset() {
  for (uint32_t i = 0; i < _num_partition; i++) {
    if (NULL != _buckets[i]) {
      _buckets[i]->clear();
    }
  }
  MemoryBlockPool::clear();
}

void MapOutputCollector::configure(Config & config) {
  _config = &config;
  MapOutputSpec::getSpecFromConfig(config, _mapOutputSpec);

  init(config.getInt("io.sort.mb", 300) * 1024 * 1024, get_comparator(config, _mapOutputSpec));

  // combiner
  const char * combinerClass = config.get(NATIVE_COMBINER);
  if (NULL != combinerClass) {
    ObjectCreatorFunc objectCreater = NativeObjectFactory::GetObjectCreator(combinerClass);
    if (NULL == objectCreater) {
      THROW_EXCEPTION_EX(UnsupportException, "Combiner not found: %s", combinerClass);
    }
    else {
      LOG("[MapOutputCollector::configure] native combiner is enabled: %s", combinerClass);
    }

    Configurable * combiner = (Configurable *)(objectCreater());
    if (NULL != combiner) {
      combiner->configure(config);
      this->_combineRunner = new CombineRunner(combiner);
    }
  }

  _collectTimer.reset();
}

ComparatorPtr MapOutputCollector::get_comparator(Config & config, MapOutputSpec & spec) {
  const char * comparatorName = config.get(NATIVE_MAPOUT_KEY_COMPARATOR);

  return NativeTask::get_comparator(spec.keyType, comparatorName);
}

/**
 * sort all partitions
 */
void MapOutputCollector::sort_partitions(SortAlgorithm sort_type, uint32_t start_partition, uint32_t end_partition) {
  // do sort
  for (uint32_t i = start_partition; i < end_partition; i++) {
    PartitionBucket * pb = _buckets[i];
    if ((NULL != pb) && (pb->current_block_idx() != NULL_BLOCK_INDEX)) {
      pb->sort(sort_type);
    }
  }
}

/**
 * Spill buffer to file
 * @return Array of spill segments information
 */
void MapOutputCollector::sort_and_spill_partitions(uint32_t start_partition,
                                     uint32_t num_partition,
                                     SortOrder orderType,
                                     SortAlgorithm sortType,
                                     IFileWriter & writer,
                                     uint64_t & blockCount,
                                     uint64_t & recordCount,
                                     uint64_t & sortTime,
                                     uint64_t & keyGroupCount) {
  if (orderType == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not supported");
  }

  uint64_t sortingTime = 0;
  Timer timer;
  uint64_t recordNum = 0;

  for (uint32_t i = 0; i < num_partition; i++) {
    writer.startPartition();
    PartitionBucket * pb = _buckets[start_partition + i];
    if (pb != NULL) {
      recordNum += pb->recored_count();
      if (orderType == FULLORDER) {
        timer.reset();
        pb->sort(sortType);
        sortingTime += timer.now() - timer.last();
      }
      pb->spill(writer, keyGroupCount);
    }
    writer.endPartition();
  }
  sortTime = sortingTime;
  recordCount = recordNum;
}


void MapOutputCollector::middle_spill(std::vector<std::string> & spillOutputs,
                                   const std::string & idx_file_path,
                                   MapOutputSpec & spec) {
  uint64_t collecttime = _collectTimer.now() - _collectTimer.last();
  if (spillOutputs.size() == 1) {
    std::string & spillPath = spillOutputs.at(0);
    uint64_t blockCount = 0;
    uint64_t recordCount = 0;
    uint64_t sortTime = 0;
    uint64_t keyGroupCount = 0;
    OutputStream * fout = FileSystem::getLocal().create(spillPath, true);
    IFileWriter * writer = new IFileWriter(fout, spec.checksumType,
                                             spec.keyType, spec.valueType,
                                             spec.codec,
                                             _spilledRecords);
    Timer timer;

    const uint32_t beginPartition = 0;
    sort_and_spill_partitions(beginPartition, _num_partition, spec.sortOrder, spec.sortAlgorithm, *writer,
                blockCount, recordCount, sortTime, keyGroupCount);
    SingleSpillInfo * info = writer->getSpillInfo();
    info->path = spillPath;
    double interval = (timer.now() - timer.last()) / 1000000000.0;

    LOG("[MapOutputCollector::mid_spill] spilling file path: %s", info->path.c_str());

    if (keyGroupCount == 0) {
      LOG("[MapOutputCollector::mid_spill] Sort and spill: {spilled file id: %d, partitions: [%u,%u), collect: %.3lfs, sort: %.3lfs, spill: %.3lfs, records: %llu, avg record size: %.3lf, blocks: %llu, uncompressed total bytes: %llu, compressed total bytes: %llu}",
          _spillInfo.getSpillCount(),
          0,
          _num_partition,
          collecttime/1000000000.0,
          sortTime/1000000000.0,
          interval - sortTime/1000000000.0,
          recordCount,
          info->getEndPosition()/(double)recordCount,
          blockCount,
          info->getEndPosition(),
          info->getRealEndPosition());
    } else {
      LOG("[MapOutputCollector::mid_spill] Sort and spill: {Spilled file id: %d,  partition: [%u,%u), collect: %.3lfs, sort: %.3lfs, spill: %.3lfs, records: %llu, key count: %llu, blocks: %llu, uncompressed total bytes: %llu, compressed total bytes: %llu}",
          _spillInfo.getSpillCount(),
          0,
          _num_partition,
          collecttime/1000000000.0,
          sortTime/1000000000.0,
          interval - sortTime/1000000000.0,
          recordCount,
          keyGroupCount,
          blockCount,
          info->getEndPosition(),
          info->getRealEndPosition());
    }

    if (idx_file_path.length()>0) {
      info->writeSpillInfo(idx_file_path);
      delete info;
    } else {
      _spillInfo.add(info);
    }
    delete writer;
    delete fout;
    reset();
    _collectTimer.reset();
  } else if (spillOutputs.size() == 0) {
    THROW_EXCEPTION(IOException, "MapOutputCollector: Spill file path empty");
  } else {
    THROW_EXCEPTION(UnsupportException, "MapOutputCollector: Parallel spill not support");
  }
}

/**
 * final merge and/or spill, use previous spilled
 * file & in-memory data
 */
void MapOutputCollector::final_merge_and_spill(std::vector<std::string> & filepaths,
                                               const std::string & idx_file_path,
                                               MapOutputSpec & spec) {
  if (_spillInfo.getSpillCount()==0) {
    middle_spill(filepaths, idx_file_path, spec);
    return;
  }
  Timer timer;
  OutputStream * fout = FileSystem::getLocal().create(filepaths[0], true);
  IFileWriter * writer = new IFileWriter(fout, spec.checksumType,
                                           spec.keyType, spec.valueType,
                                           spec.codec,
                                           _spilledRecords);
  Merger * merger = new Merger(writer, *_config, _keyComparator, _combineRunner);
  InputStream ** inputStreams = new InputStream*[_spillInfo.getSpillCount()];
  IFileReader ** readers = new IFileReader*[_spillInfo.getSpillCount()];
  for (size_t i = 0 ; i < _spillInfo.getSpillCount() ; i++) {
    SingleSpillInfo * spill = _spillInfo.getSingleSpillInfo(i);
    inputStreams[i] = FileSystem::getLocal().open(spill->path);
    readers[i] = new IFileReader(inputStreams[i], spec.checksumType,
                                  spec.keyType, spec.valueType,
                                  spill, spec.codec);
    MergeEntryPtr pme = new IFileMergeEntry(readers[i]);
    merger->addMergeEntry(pme);
  }

  LOG("[MapOutputCollector::final_merge_and_spill] Spilling file path: %s", filepaths[0].c_str());
  if (spec.sortOrder==GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not support");
  } else if (spec.sortOrder==FULLORDER) {
    timer.reset();
    sort_partitions(spec.sortAlgorithm, 0, _num_partition);
    LOG("[MapOutputCollector::final_merge_and_spill]  Sort:{spilling file id: %d, partitions: [%u,%u), sort time: %.3lf s}",
        _spillInfo.getSpillCount() + 1,
        0,
        _num_partition,
        (timer.now()-timer.last())/1000000000.0);
  }
  merger->addMergeEntry(new MemoryMergeEntry(this));

  timer.reset();
  merger->merge();

  delete merger;
  for (size_t i=0;i<_spillInfo.getSpillCount();i++) {
    delete readers[i];
    delete inputStreams[i];
  }
  delete [] readers;
  delete [] inputStreams;
  delete fout;
  // write index
  SingleSpillInfo * spill_range = writer->getSpillInfo();
  spill_range->writeSpillInfo(idx_file_path);
  delete spill_range;
  delete_temp_spill_files();
  reset();

  LOG("[MapOutputCollector::final_merge_and_spill]  Merge and Spill:{spilled file id: %d, merge and spill time: %.3lf s}",
          _spillInfo.getSpillCount(),
          (timer.now()-timer.last())/1000000000.0);

}

uint64_t MapOutputCollector::estimate_spill_size(OutputFileType output_type,
    KeyValueType ktype, KeyValueType vtype) {
  uint64_t ret = 0;
  if (_buckets) {
    for (uint32_t i = 0; i < _num_partition; i++) {
      ret += _buckets[i]->estimate_spill_size(output_type, ktype, vtype);
    }
  }
  return ret;
}


}; // namespace NativeTask





