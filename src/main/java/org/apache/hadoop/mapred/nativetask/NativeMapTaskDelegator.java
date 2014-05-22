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
import org.apache.hadoop.mapred.nativetask.handlers.NativeMapAndCollectorHandler;
import org.apache.hadoop.mapred.nativetask.handlers.NativeMapOnlyHandlerNoReducer;
import org.apache.hadoop.mapred.nativetask.handlers.NativeMapTask;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.mapred.nativetask.util.NativeTaskOutput;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;

@SuppressWarnings("unchecked")
public class NativeMapTaskDelegator<INKEY, INVALUE, OUTKEY, OUTVALUE> implements TaskDelegation.MapTaskDelegator {
  private static final Log LOG = LogFactory.getLog(NativeMapTaskDelegator.class);

  private JobConf job = null;

  private TaskReporter reporter;

  public NativeMapTaskDelegator() {
  }

  private static <T> byte[] serialize(Configuration conf, Object obj) throws IOException {
    final SerializationFactory factory = new SerializationFactory(conf);

    final Serializer<T> serializer = (Serializer<T>) factory.getSerializer(obj.getClass());
    final DataOutputBuffer out = new DataOutputBuffer(1024);
    serializer.open(out);

    serializer.serialize((T) obj);
    final byte[] ret = new byte[out.getLength()];
    System.arraycopy(out.getData(), 0, ret, 0, out.getLength());
    return ret;
  }

  @Override
  public void init(TaskUmbilicalProtocol protocol, TaskReporter reporter, Configuration conf) throws Exception {

    this.job = new JobConf(conf);

    Platforms.init(conf);

    this.reporter = reporter;
    
    NativeRuntime.configure(job);
  }

  @Override
  public void run(TaskAttemptID taskAttemptID, Object split) throws IOException {
    final long updateInterval = job.getLong(Constants.NATIVE_STATUS_UPDATE_INTERVAL, 1000);
    final StatusReportChecker statusChecker = new StatusReportChecker(reporter, updateInterval);
    statusChecker.start();

    if (job.get(Constants.NATIVE_RECORDREADER_CLASS) != null) {

      // delegate entire map task
      final byte[] splitData = serialize(job, split);
      job.set(Constants.NATIVE_INPUT_SPLIT, BytesUtil.fromBytes(splitData));
      final NativeMapTask processor = NativeMapTask.create(job, taskAttemptID);

      try {
        processor.run();
      } catch (final Exception e) {
        throw new IOException(e);
      } finally {
        processor.close();
      }
    } else {

      final RecordReader<INKEY, INVALUE> rawIn = job.getInputFormat()
          .getRecordReader((InputSplit) split, job, reporter);

      final INKEY key = rawIn.createKey();
      final INVALUE value = rawIn.createValue();


      final Class<INKEY> ikeyClass = (Class<INKEY>) key.getClass();
      final Class<INVALUE> ivalueClass = (Class<INVALUE>) value.getClass();
      final Class<OUTKEY> okeyClass = (Class<OUTKEY>) job.getOutputKeyClass();
      final Class<OUTVALUE> ovalueClass = (Class<OUTVALUE>) job.getOutputValueClass();

      final int numReduceTasks = job.getNumReduceTasks();
      TaskContext context = new TaskContext(job, ikeyClass, ivalueClass, okeyClass, ovalueClass, reporter,  taskAttemptID);
      
      if (numReduceTasks > 0) {
        
        final NativeMapAndCollectorHandler<INKEY, INVALUE, OUTKEY, OUTVALUE> processor = NativeMapAndCollectorHandler
            .create(context);

        try {
          while (rawIn.next(key, value)) {
            processor.collect(key, value);
          }
        } finally {
          processor.close();
        }
      } else {
        NativeTaskOutput output = OutputUtil.createNativeTaskOutput(job, taskAttemptID.toString());
        final String finalName = output.getOutputName(taskAttemptID.getTaskID().getId());
        final FileSystem fs = FileSystem.get(job);
        final RecordWriter writer = job.getOutputFormat().getRecordWriter(fs, job, finalName,
            reporter);

        final NativeMapOnlyHandlerNoReducer processor = NativeMapOnlyHandlerNoReducer
            .create(context, writer);

        try {
          while (rawIn.next(key, value)) {
            processor.collect(key, value);
          }
          writer.close(reporter);
        } finally {
          processor.close();
        }
      }
    }

    try {
      statusChecker.stop();
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
    // final update
    NativeRuntime.reportStatus(reporter);
  }
}
