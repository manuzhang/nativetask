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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskDelegation;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.nativetask.handlers.AllNativeMapTask;
import org.apache.hadoop.mapred.nativetask.handlers.NativeMapAndCollectHandler;
import org.apache.hadoop.mapred.nativetask.handlers.NativeMapOnlyHandler;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;

public class NativeMapTaskDelegator<INKEY, INVALUE, OUTKEY, OUTVALUE>
    implements TaskDelegation.MapTaskDelegator {
  private static final Log LOG = LogFactory
      .getLog(NativeMapTaskDelegator.class);
  
  private JobConf job = null;

  public NativeMapTaskDelegator() {
  }

  private static <T> byte[] serialize(Configuration conf, Object obj)
      throws IOException {
    SerializationFactory factory = new SerializationFactory(conf);
    
    @SuppressWarnings({ "unchecked"})
    Serializer<T> serializer = (Serializer<T>) factory.getSerializer(obj
        .getClass());
    DataOutputBuffer out = new DataOutputBuffer(1024);
    serializer.open(out);
    
    serializer.serialize((T) obj);
    byte[] ret = new byte[out.getLength()];
    System.arraycopy(out.getData(), 0, ret, 0, out.getLength());
    return ret;
  }

  @Override
  public void setConf(Configuration conf) {
    this.job = new JobConf(conf);
  }

  @Override
  public Configuration getConf() {
    return this.job;
  }

  @Override
  public void run(TaskAttemptID taskAttemptID, 
      TaskUmbilicalProtocol umbilical, TaskReporter reporter,
      Object split) throws IOException {
    long updateInterval = job.getLong("native.update.interval", 1000);
    StatusReportChecker updater = new StatusReportChecker(reporter,
        updateInterval);
    updater.startUpdater();
    NativeRuntime.configure(job);

    if (job.get(Constants.NATIVE_RECORDREADER_CLASS) != null) {
      
      // delegate entire map task
      byte[] splitData = serialize(job, split);
      NativeRuntime.configure("native.input.split", splitData);
      AllNativeMapTask processor = new AllNativeMapTask(
          job, taskAttemptID);
      processor.init(job);

      try {
        processor.run();
      } catch (Exception e) {
        throw new IOException(e);
      } finally {
        processor.close();
      }
    } else {
      RecordReader<INKEY, INVALUE> rawIn = job.getInputFormat()
          .getRecordReader((InputSplit) split, job, reporter);

      INKEY key = rawIn.createKey();
      INVALUE value = rawIn.createValue();
      Class<INKEY> ikeyClass = (Class<INKEY>) key.getClass();
      Class<INVALUE> ivalueClass = (Class<INVALUE>) value.getClass();

      int bufferCapacity = job.getInt(Constants.NATIVE_PROCESSOR_BUFFER_KB,
          Constants.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;

      int numReduceTasks = job.getNumReduceTasks();
      LOG.info("numReduceTasks: " + numReduceTasks);
      if (numReduceTasks > 0) {
        NativeMapAndCollectHandler<INKEY, INVALUE> processor = new NativeMapAndCollectHandler<INKEY, INVALUE>(
            bufferCapacity, ikeyClass, ivalueClass, job, taskAttemptID);

        processor.init(job);
        try {
          while (rawIn.next(key, value)) {
            processor.process(key, value);
          }
        } finally {
          processor.close();
        }
      } else {
        String finalName = OutputPathUtil.getOutputName(taskAttemptID
            .getTaskID().getId());
        FileSystem fs = FileSystem.get(job);
        
        @SuppressWarnings("unchecked")
        RecordWriter<OUTKEY, OUTVALUE> writer = job.getOutputFormat()
            .getRecordWriter(fs, job, finalName, reporter);
        
        @SuppressWarnings("unchecked")
        Class<OUTKEY> okeyClass = (Class<OUTKEY>) job.getOutputKeyClass();
        
        @SuppressWarnings("unchecked")
        Class<OUTVALUE> ovalueClass = (Class<OUTVALUE>) job
            .getOutputValueClass();
        NativeMapOnlyHandler<INKEY, INVALUE, OUTKEY, OUTVALUE> processor = new NativeMapOnlyHandler<INKEY, INVALUE, OUTKEY, OUTVALUE>(
            bufferCapacity, bufferCapacity, ikeyClass, ivalueClass, okeyClass,
            ovalueClass, job, writer);
        processor.init(job);

        try {
          while (rawIn.next(key, value)) {
            processor.process(key, value);
          }
          writer.close(reporter);
        } finally {
          processor.close();
        }
      }
    }

    try {
      updater.stopUpdater();
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
    // final update
    NativeRuntime.reportStatus(reporter);
  }
}
