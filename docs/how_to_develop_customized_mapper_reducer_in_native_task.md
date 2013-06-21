Introduction
============

This tutorial will cover the following items:
1. native interface for native mapper, reducer, and etc.
2. native task configurations
3. How to develop customized native mapper, native reducer, and how to wrap them into a customized native library.
4. How to submit customized native libraries to jobtracker containing native mapper and native reducer.
5. How to use build-in native terasort, and wortcount
6. How to use streaming in nativetask.
7. A full code example containing all the code for a native library.

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

Job submission
===========

## Usage
TBD

## Examples
### Terasort
Test for Terasort  
InputData 200G compressed(44G)  
map.tasks=200 mapred.output.compression.codec=Snappy  
cmds:
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar teragen 2000000000 /tera200G-snappy  
bin/hadoop jar lib/hadoop-nativetask-0.1.0.jar terasort /tera200G-snappy /terasort200G-nt-300  
bin/hadoop jar lib/hadoop-nativetask-0.1.0.jar terasort /tera200G-snappy /terasort200G-nt  

### WordCount
Test for WordCount  
InputData 100G compressed(52G)  
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar randomtextwriter -Dtest.randomtextwrite.total_bytes=100000000000 -Dtest.randomtextwrite.bytes_per_map=500000000 -outFormat org.apache.hadoop.mapred.TextOutputFormat /text100G-snappy  
map.tasks=200 mapred.output.compression.codec=Snappy  
cmds:  
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar wordcount /text100G-snappy /wordcount-java-300-opt  
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar wordcount -Dwordcount.enable.fast.mapper=true /text100G-snappy /wordcount-java-300-opt  
bin/hadoop jar lib/hadoop-nativetask-0.1.0.jar -reader NativeTask.LineRecordReader -writer NativeTask.TextIntRecordWriter -mapper NativeTask.WordCountMapper -reducer NativeTask.IntSumReducer -combiner NativeTask.IntSumReducer -input /text100G-snappy -output /wordcount-100G-nt  
  
## Streaming
streaming cmd:  
bin/hadoop jar hadoop-nativetask-0.1.0.jar -lib Streaming=libstreaming.dylib -reader Streaming.StreamingReader -writer Streaming.StreamingWriter -mapper Streaming.MStreamingMapper -reducer Streaming.MStreamingReducer -input terainput/part-00000 -output streamingoutput  

## Customized native libraries
``TBD``

Attachment: Full code example
========
<pre></code>
TBD
</code></pre>
