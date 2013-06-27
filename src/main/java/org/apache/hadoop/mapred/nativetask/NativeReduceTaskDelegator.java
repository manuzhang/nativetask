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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskDelegation;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.nativetask.handlers.NativeReduceAndWriteHandler;
import org.apache.hadoop.mapred.nativetask.handlers.NativeReduceOnlyHandler;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;

public class NativeReduceTaskDelegator<IK, IV, OK, OV> implements
    TaskDelegation.ReduceTaskDelegator {
  private static final Log LOG = LogFactory
      .getLog(NativeReduceTaskDelegator.class);
  private JobConf job;

  public NativeReduceTaskDelegator() {
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
  public void run(TaskAttemptID taskAttemptID, TaskUmbilicalProtocol umbilical,
      TaskReporter reporter, RawKeyValueIterator rIter,
      RawComparator comparator, Class keyClass, Class valueClass)
      throws IOException {
    long updateInterval = job.getLong("native.update.interval", 1000);
    StatusReportChecker updater = new StatusReportChecker(reporter,
        updateInterval);
    updater.startUpdater();
    NativeRuntime.configure(job);
    
    int bufferCapacity = job.getInt(Constants.NATIVE_PROCESSOR_BUFFER_KB,
        Constants.NATIVE_PROCESSOR_BUFFER_KB_DEFAULT) * 1024;
    String finalName = OutputPathUtil.getOutputName(taskAttemptID.getTaskID()
        .getId());

    if (job.get("native.recordwriter.class") != null) {
      // delegate whole reduce task
      NativeRuntime.configure("native.output.file.name", finalName);
      NativeReduceAndWriteHandler<IK, IV> processor = new NativeReduceAndWriteHandler<IK, IV>(
          bufferCapacity, 0, keyClass, valueClass, job, reporter, rIter);
      processor.init(job);
      processor.run();
      processor.close();
    } else {
      FileSystem fs = FileSystem.get(job);
      RecordWriter<OK, OV> writer = job.getOutputFormat().getRecordWriter(fs,
          job, finalName, reporter);
      Class<OK> okeyClass = (Class<OK>) job.getOutputKeyClass();
      Class<OV> ovalueClass = (Class<OV>) job.getOutputValueClass();
      NativeReduceOnlyHandler<IK, IV, OK, OV> processor = new NativeReduceOnlyHandler<IK, IV, OK, OV>(
          bufferCapacity, bufferCapacity, keyClass, valueClass, okeyClass,
          ovalueClass, job, writer, reporter, rIter);
      processor.init(job);
      processor.run();
      writer.close(reporter);
    }

    try {
      updater.stopUpdater();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    // final update
    NativeRuntime.reportStatus(reporter);
  }
}
