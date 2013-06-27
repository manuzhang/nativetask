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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.nativetask.NativeDataReader;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.util.Progressable;

/**
 * 
 * Java record reader + Native reducer + Native Record Writer.
 * 
 * @param <IK>
 * @param <IV>
 */
public class NativeReduceAndWriteHandler<IK, IV> extends NativeReduceOnlyHandler<IK, IV, Writable, Writable> {

  public NativeReduceAndWriteHandler(int inputBufferCapacity,
      int outputBufferCapacity, Class<IK> iKClass, Class<IV> iVClass,
      JobConf conf, Progressable progress, RawKeyValueIterator rIter)
      throws IOException {
    super(inputBufferCapacity, outputBufferCapacity, iKClass, iVClass, null,
        null, conf, null, progress, rIter);
  }
  
  public void run() throws IOException {
    sendCommandToNative(BytesUtil.toBytes("run"));
  }
  
  @Override
  protected boolean flushOutputAndProcess(NativeDataReader reader, int length)
      throws IOException {
    //we don't expect to do anything here.
    return true;
  }

}