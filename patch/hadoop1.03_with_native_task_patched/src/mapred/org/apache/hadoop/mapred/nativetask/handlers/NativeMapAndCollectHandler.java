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
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;

/**
 * 
 * Java Record Reader + Native Mapper + Native Collector
 * 
 * @param <IK>
 * @param <IV>
 */
public class NativeMapAndCollectHandler<IK, IV> extends NativeBatchProcessor<IK, IV, Writable, Writable> {

  private static final Log LOG = LogFactory
      .getLog(NativeMapAndCollectHandler.class);

  private OutputPathUtil mapOutputFile;
  private int spillNumber = 0;
  
  public NativeMapAndCollectHandler(int bufferCapacity, Class<IK> keyClass,
      Class<IV> valueClass, Configuration conf, TaskAttemptID taskAttemptID)
      throws IOException {
    super(keyClass, 
        valueClass, 
        null, 
        null, 
        "NativeTask.MMapperHandler",
        bufferCapacity, 
        0);
    
    this.mapOutputFile = new OutputPathUtil();
    this.mapOutputFile.setConf(conf);
  }
  
  @Override
  public void init(Configuration conf) throws IOException {
    super.init(conf);
  }
  
  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    String cmd = BytesUtil.fromBytes(data);
    Path p = null;
    if (cmd.equals("GetOutputPath")) {
      p = mapOutputFile.getOutputFileForWrite(-1);
    } else if (cmd.equals("GetOutputIndexPath")) {
      p = mapOutputFile.getOutputIndexFileForWrite(-1);
    } else if (cmd.equals("GetSpillPath")) {
      p = mapOutputFile.getSpillFileForWrite(spillNumber++, -1);
    } else {
      LOG.warn("Illegal command: " + cmd);
    }
    if (p != null) {
      return BytesUtil.toBytes(p.toUri().getPath());
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }

  public void process(IK key, IV value) throws IOException {
    serializer.serializeKV(nativeWriter, (Writable) key, (Writable) value);
  };

  @Override
  public void close() throws IOException {
    super.close();
  }
}