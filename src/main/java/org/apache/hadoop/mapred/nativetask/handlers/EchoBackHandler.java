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
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

/**
 * Echo input back to output
 * 
 */
class EchoBackHandler extends NativeBatchProcessor<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
  public ArrayList<byte[]> tempOutput = new ArrayList<byte[]>();
  public boolean finished = false;
  static int BUFFER_SIZE = 128 * 1024;

  public EchoBackHandler() throws IOException {
    super(BytesWritable.class, 
        BytesWritable.class, 
        BytesWritable.class,
        BytesWritable.class, 
        "NativeTask.EchoBatchHandler", 
        BUFFER_SIZE,
        BUFFER_SIZE);
  }

  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    return BytesUtil.toBytes("Java:" + BytesUtil.fromBytes(data));
  }
}