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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskDelegation;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.INativeComparable;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;

/**
 * 
 * Java Record Reader + Java Mapper + Native Collector
 * 
 * @param <K>
 * @param <V>
 */
public class NativeMapOutputCollector<K extends Writable, V extends Writable>
    extends NativeBatchProcessor<K, V, Writable, Writable> implements
    TaskDelegation.MapOutputCollectorDelegator<K, V> {
  private static Log LOG = LogFactory.getLog(NativeMapOutputCollector.class);

  private OutputPathUtil outputFileUtil = null;
  private int spillNumber = 0;

  public static boolean canEnable(JobConf job) {
    if (!job.getBoolean(Constants.NATIVE_MAPOUTPUT_COLLECTOR_ENABLED, false)) {
      return false;
    }
    if (job.getNumReduceTasks() == 0) {
      return false;
    }
    if (job.getCombinerClass() != null) {
      return false;
    }
    if (job.getClass("mapred.output.key.comparator.class", null,
        RawComparator.class) != null) {
      return false;
    }
    if (job.getBoolean("mapred.compress.map.output", false) == true) {
      if (!"org.apache.hadoop.io.compress.SnappyCodec".equals(job
          .get("mapred.map.output.compression.codec"))) {
        return false;
      }
    }
    
    Class<?> keyCls = job.getMapOutputKeyClass();
    try {
      INativeSerializer serializer = NativeSerialization.getInstance().getSerializer(keyCls);
      if (! (serializer instanceof INativeComparable)) {
        return false;
      }
    } catch (IOException e) {
      LOG.error("Not supported key type " + ((null == keyCls) ? null : keyCls.getName()) , e);
      return false;
    }
    
    boolean ret = NativeRuntime.isNativeLibraryLoaded();
    if (ret) {
      NativeRuntime.configure(job);
    }
    LOG.info("NativeRuntime.isNativeLibraryLoaded():" + ret);
    LOG.info("Native task can enable:" + ret);

    return ret;
  }

  @SuppressWarnings("unchecked")
  public NativeMapOutputCollector(JobConf jobConf, TaskAttemptID taskAttemptID)
      throws IOException {
    super((Class<K>)jobConf.getMapOutputKeyClass(), 
        (Class<V>)(jobConf.getMapOutputValueClass()),
        null, 
        null, 
        "NativeTask.MCollectorOutputHandler", 
        jobConf.getInt(Constants.NATIVE_PROCESSOR_BUFFER_KB, 1024) * 1024, 
        0);
    
    this.outputFileUtil = new OutputPathUtil();
    outputFileUtil.setConf(jobConf);
  }
  
  @Override
  public void collect(K key, V value, int partition) throws IOException,
      InterruptedException {

    serializer.serializeKV(nativeWriter, key, value);
    nativeWriter.writeInt(partition);
  };

  @Override
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
    } else {
      LOG.warn("Illegal command: " + cmd);
    }
    if (p != null) {
      return BytesUtil.toBytes(p.toUri().getPath());
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }
}
