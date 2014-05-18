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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.DataReceiver;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.util.Progressable;

/**
 * Native Reducer
 */
public class NativeReduceTask<IK, IV, OK, OV> implements CommandDispatcher, DataReceiver, Closeable {

  public static String NAME = "NativeTask.RReducerHandler";
  
  public static Command LOAD = new Command(1, "Load");
  public static Command RUN = new Command(3, "RUN");

  
  byte[] outputLength = new byte[4];
  private final BufferPushee<OK, OV> kvWriter;
  private final BufferPullee<IK, IV> kvLoader;
  private final INativeHandler nativeHandler;
  private final InputBuffer in;
  private boolean closed = false;

  public static <IK, IV, OK, OV> NativeReduceTask<IK, IV, OK, OV> create(Class<IK> iKClass, Class<IV> iVClass,
      Class<OK> oKClass, Class<OV> oVClass, JobConf conf, RecordWriter<OK, OV> writer, Progressable progress,
      RawKeyValueIterator rIter) throws IOException {

    final INativeHandler nativeHandler = NativeBatchProcessor.create(NAME, conf, DataChannel.INOUT);
    BufferPushee<OK, OV> kvWriter = null;
    if (null != writer) {
      kvWriter = new BufferPushee<OK, OV>(oKClass, oVClass, writer);
    }
    final BufferPullee<IK, IV> kvLoader = new BufferPullee<IK, IV>(iKClass, iVClass, rIter, nativeHandler);
    return new NativeReduceTask<IK, IV, OK, OV>(nativeHandler, kvWriter, kvLoader);
  }

  protected NativeReduceTask(INativeHandler nativeHandler, BufferPushee<OK, OV> kvWriter, BufferPullee<IK, IV> kvLoader)
      throws IOException {
    this.kvWriter = kvWriter;
    this.kvLoader = kvLoader;
    this.nativeHandler = nativeHandler;
    this.in = nativeHandler.getInputBuffer();
    nativeHandler.setDataReceiver(this);
    nativeHandler.setCommandDispatcher(this);
  }

  @Override
  public boolean receiveData() throws IOException {
    if (null == kvWriter) {
      return true;
    } else {
      return kvWriter.collect(in);
    }
  }

  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
    if (null == command) {
      return null;
    }
    if (command.equals(LOAD)) {
      final ReadWriteBuffer result = new ReadWriteBuffer(4);
      result.writeInt(kvLoader.load());
      return result;
    } else {
      throw new IOException("command not support");
    }
  }

  public void run() throws IOException {
    nativeHandler.call(RUN, null);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != kvWriter) {
      kvWriter.close();
    }
    if (null != kvLoader) {
      kvLoader.close();
    }
    if (null != nativeHandler) {
      nativeHandler.close();
    }
    closed = true;
  }
}