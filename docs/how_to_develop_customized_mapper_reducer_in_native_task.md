Introduction
============

``TBD``

Interfaces
============
You need to include "NativeTask.h".
## Mapper

*NativeTask.h*
<pre><code>
class Mapper: public ProcessorBase {
public:
  virtual NativeObjectType type() {
    return MapperType;
  }

  /**
   * Map interface, default IdenticalMapper
   */
  virtual void map(const char * key, uint32_t keyLen,
                   const char * value, uint32_t valueLen) {
    collect(key, keyLen, value, valueLen);
  }
};
</code></pre>

``TBD: How to support customized namespace??``

## Reducer
*NativeTask.h*
<pre><code>
class Reducer: public ProcessorBase {
public:
  virtual NativeObjectType type() {
    return ReducerType;
  }

  /**
   * Reduce interface, default IdenticalReducer
   */
  virtual void reduce(KeyGroup & input) {
    const char * key;
    const char * value;
    uint32_t keyLen;
    uint32_t valueLen;
    key = input.getKey(keyLen);
    while (NULL != (value=input.nextValue(valueLen))) {
      collect(key, keyLen, value, valueLen);
    }
  }
};

</code></pre>

## Comparator

``TBD``

## Partitioner
<pre><code>
class Partitioner: public Configurable {
public:
  virtual NativeObjectType type() {
    return PartitionerType;
  }

  /**
   * Partition interface
   * @param key key buffer
   * @param keyLen key length, can be modified to smaller value
   *               to truncate key
   * @return partition number
   */
  virtual uint32_t getPartition(const char * key, uint32_t & keyLen,
      uint32_t numPartition);
};
</code></pre>
## Record Writer

<pre><code>
class RecordWriter : public Collector, public Configurable {
public:
  virtual NativeObjectType type() {
    return RecordWriterType;
  }

  virtual void collect(const void * key, uint32_t keyLen,
                     const void * value, uint32_t valueLen) {}

  virtual void close() {}

};
</code></pre>

## Record Reader

<pre><code>
class RecordReader:
  public KVIterator, public Configurable, public Progress {
public:
  virtual NativeObjectType type() {
    return RecordReaderType;
  }

  virtual bool next(Buffer & key, Buffer & value) = 0;

  virtual float getProgress() = 0;

  virtual void close() = 0;
};

</code></pre>
## Combiner


Steps to develop custom library
============
### 1. Define customized mapper and reducer(or other class) in native
  *. include "NativeTask.h"
  *. Define mapper
``TBD``

### 2. Define custom libraries in native
<pre><code>

#include "NativeTask.h"

DEFINE_NATIVE_LIBRARY(CustomizedLibrary) {
  REGISTER_CLASS(CustomMapperName, CustomizedLibrary);
  REGISTER_CLASS(CustomReducerName, CustomizedLibrary);
  REGISTER_CLASS(CustomPartitionerName, CustomizedLibrary);
  REGISTER_CLASS(CustomFolderName, CustomizedLibrary);
  REGISTER_CLASS(CustomLineRecordReaderName, CustomizedLibrary);
  REGISTER_CLASS(CustomKeyValueLineRecordReaderName, CustomizedLibrary);
  REGISTER_CLASS(CustomLineRecordWriterName, CustomizedLibrary);

</code></pre>

The handler name for CustomMapper Mapper will be "CustomizedLibrary.CustomMapperName". 
The mapper can be created on the fly on the java side by 

### 3. Build libraries to get xx.so


### 4. Register the library in java 
`NativeRuntime.registerLibrary("path_to_customized_library_so", "CustomizedLibrary")`


### 5: Config custom mapper and custom reducer

For mapper, we need to define
`"native.mapper.class"` using ``NativeRuntime.configure("native.mapper.class", "CustomMapperName")``

For a full list of configuration, check [Configuration](#configuration) section.

Configuration
============
* native.mapper.class
* native.reducer.class
* native.recordreader.class
* native.recordwriter.class
* native.combiner.class
* native.partitioner.class
* native.sort.type
* native.input.split
* native.output.file.name
* native.spill.sort.first
* native.log.device
* native.class.library
* native.hadoop.version

Example
===========
Here is a full example:
<pre></code>
TBD
</code></pre>
