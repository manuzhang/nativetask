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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
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

  @SuppressWarnings("unchecked")
  public NativeCollectorOnlyHandler(JobConf jobConf) throws IOException {
    super((Class<K>) jobConf.getMapOutputKeyClass(), (Class<V>) (jobConf
        .getMapOutputValueClass()), null, null,
        "NativeTask.MCollectorOutputHandler", jobConf.getInt(
            Constants.NATIVE_PROCESSOR_BUFFER_KB, 1024) * 1024, 0);

    this.outputFileUtil = new OutputPathUtil();
    outputFileUtil.setConf(jobConf);
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
