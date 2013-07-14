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

package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;

/**
 * 
 * Java Record Reader + Java Mapper + Native Collector
 * 
 * @param <K>
 * @param <V>
 */
public class NativeCollectorOnlyHandler<K extends Writable, V extends Writable>
    extends NativeBatchProcessor<K, V, Writable, Writable> {
  private static Log LOG = LogFactory.getLog(NativeCollectorOnlyHandler.class);

  private OutputPathUtil outputFileUtil = null;
  private int spillNumber = 0;
  private ICombineHandler combinerHandler = null;

  private Class<K> outputKeyClass;

  private Class<V> outputValueClass;

  private int bufferSize;

  private JobConf jobConf;

  private TaskReporter reporter;

  private TaskAttemptID taskAttemptID;

  @SuppressWarnings("unchecked")
  public NativeCollectorOnlyHandler(JobConf jobConf, 
      TaskReporter reporter, TaskAttemptID taskAttemptID) throws IOException {
    super((Class<K>) jobConf.getMapOutputKeyClass(), (Class<V>) (jobConf
        .getMapOutputValueClass()), null, null,
        "NativeTask.MCollectorOutputHandler", jobConf.getInt(
            Constants.NATIVE_PROCESSOR_BUFFER_KB, 1024) * 1024, 0);

    this.outputFileUtil = new OutputPathUtil();
    outputFileUtil.setConf(jobConf);
    
    this.outputKeyClass = (Class<K>) jobConf.getMapOutputKeyClass();
    this.outputValueClass = (Class<V>) (jobConf.getMapOutputValueClass());
    this.jobConf = jobConf;
    
    this.reporter = reporter;
    this.taskAttemptID = taskAttemptID;
    
    this.bufferSize = jobConf.getInt(
        Constants.NATIVE_PROCESSOR_BUFFER_KB, 1024) * 1024;
  }

  @Override
  public void init(Configuration conf) throws IOException {
    
    this.combinerHandler = CombinerHandler.create(conf, outputKeyClass, outputValueClass,
        bufferSize, bufferSize, reporter, taskAttemptID);
    super.init(conf);
  }
  
  public void collect(K key, V value, int partition) throws IOException,
      InterruptedException {
    serializer.serializeKV(nativeWriter, key, value);
    nativeWriter.writeInt(partition);
  };

  public void flush() throws IOException, InterruptedException {
    nativeWriter.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    String cmd = BytesUtil.fromBytes(data);
    Path p = null;
    if (cmd.equals("GetOutputPath")) {
      p = outputFileUtil.getOutputFileForWrite(-1);
    } else if (cmd.equals("GetOutputIndexPath")) {
      p = outputFileUtil.getOutputIndexFileForWrite(-1);
    } else if (cmd.equals("GetSpillPath")) {
      p = outputFileUtil.getSpillFileForWrite(spillNumber++, -1);
    } else if (cmd.equals("getCombinerHandler")) {
      if (null == combinerHandler) {
        return null;
      }
      byte[] result = new byte[8];
      return toBytes(combinerHandler.getId(), result);
    } else {
      LOG.warn("Illegal command: " + cmd);
    }
    if (p != null) {
      return BytesUtil.toBytes(p.toUri().getPath());
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }
  
  // same rule as DataOutputStream
  private static byte[] toBytes(long v, byte[] b) {
    int upper = (int)((v >>> 32) & 0x0FFFFFFFF);
    int lower = (int)(v & 0x0FFFFFFFF);
    b[7] = (byte) ((upper >>> 24) & 0xFF);
    b[6] = (byte) ((upper >>> 16) & 0xFF);
    b[5] = (byte) ((upper >>> 8) & 0xFF);
    b[4] = (byte) ((upper >>> 0) & 0xFF);
    
    b[3] = (byte) ((lower >>> 24) & 0xFF);
    b[2] = (byte) ((lower >>> 16) & 0xFF);
    b[1] = (byte) ((lower >>> 8) & 0xFF);
    b[0] = (byte) ((lower >>> 0) & 0xFF);
    return b;
  }
}
