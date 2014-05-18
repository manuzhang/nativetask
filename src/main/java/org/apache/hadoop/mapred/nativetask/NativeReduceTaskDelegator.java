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
import org.apache.hadoop.mapred.nativetask.handlers.NativeReduceTask;
import org.apache.hadoop.mapred.nativetask.util.NativeTaskOutput;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;

public class NativeReduceTaskDelegator<IK, IV, OK, OV> implements TaskDelegation.ReduceTaskDelegator {
  private static final Log LOG = LogFactory.getLog(NativeReduceTaskDelegator.class);

  private JobConf job;
  private TaskReporter reporter;

  public NativeReduceTaskDelegator() {
  }

  @Override
  public void init(TaskUmbilicalProtocol protocol, TaskReporter reporter, Configuration conf) throws Exception {
    this.job = new JobConf(conf);

    Platforms.init(conf);

    this.reporter = reporter;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void run(TaskAttemptID taskAttemptID, RawKeyValueIterator rIter, RawComparator comparator, Class keyClass,
      Class valueClass) throws IOException {
    final long updateInterval = job.getLong(Constants.NATIVE_STATUS_UPDATE_INTERVAL,
        Constants.NATIVE_STATUS_UPDATE_INTERVAL_DEFVAL);
    final StatusReportChecker updater = new StatusReportChecker(reporter, updateInterval);
    updater.start();
    NativeRuntime.configure(job);

    NativeTaskOutput output = OutputUtil.createNativeTaskOutput(job, taskAttemptID.toString());
    final String finalName = output.getOutputName(taskAttemptID.getTaskID().getId());

    LOG.info("Delegeate reduce function to native space: ");

    final FileSystem fs = FileSystem.get(job);

    RecordWriter<OK, OV> writer = null;
    if (job.get(Constants.NATIVE_RECORDWRITER_CLASS) == null) {
      // use java record writer
      writer = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
    } else {
      // configure the native record writer output file name
      job.set(Constants.NATIVE_OUTPUT_FILE_NAME, finalName);
    }

    final Class<OK> okeyClass = (Class<OK>) job.getOutputKeyClass();
    final Class<OV> ovalueClass = (Class<OV>) job.getOutputValueClass();

    final NativeReduceTask processor = NativeReduceTask.create(keyClass, valueClass, okeyClass, ovalueClass, job,
        writer, reporter, rIter);
    processor.run();

    try {
      updater.stop();
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
    // final update
    NativeRuntime.reportStatus(reporter);
  }
}
