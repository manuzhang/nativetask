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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.DataReceiver;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;

/**
 * Java Record Reader + Native Mapper
 */

public class NativeMapOnlyHandlerNoReducer<IK, IV, OK, OV> implements DataReceiver, Closeable {

  private static final Log LOG = LogFactory.getLog(NativeMapOnlyHandlerNoReducer.class);

  public static String NAME = "NativeTask.MMapperHandler";
  private final BufferPushee<OK, OV> kvWriter;
  private final BufferPusher<IK, IV> kvPusher;
  private final INativeHandler nativeHandler;

  private boolean closed = false;
  private final InputBuffer in;

  public static <IK, IV, OK, OV> NativeMapOnlyHandlerNoReducer<IK, IV, OK, OV> create(TaskContext context,
      RecordWriter<OK, OV> writer) throws IOException {

    LOG.info("[NativeTask] Map only job, delegate to native implementation");

    final INativeHandler nativeHandler = NativeBatchProcessor.create(NAME, context.getConf(), DataChannel.INOUT);
    final BufferPushee<OK, OV> kvWriter = new BufferPushee<OK, OV>(context.getOuputKeyClass(), context.getOutputValueClass(),
        writer);
    final BufferPusher<IK, IV> kvPusher = new BufferPusher<IK, IV>(context.getInputKeyClass(),
        context.getInputValueClass(), nativeHandler);
    return new NativeMapOnlyHandlerNoReducer<IK, IV, OK, OV>(nativeHandler, kvPusher, kvWriter);
  }

  protected NativeMapOnlyHandlerNoReducer(INativeHandler nativeHandler, BufferPusher<IK, IV> kvPusher,
      BufferPushee<OK, OV> writer) throws IOException {
    this.kvWriter = writer;
    this.kvPusher = kvPusher;
    this.nativeHandler = nativeHandler;
    nativeHandler.setDataReceiver(this);
    this.in = nativeHandler.getInputBuffer();
  }

  @Override
  public boolean receiveData() throws IOException {
    if (null != kvWriter) {
      return kvWriter.collect(in);
    } else {
      return true;
    }
  }

  public void collect(IK key, IV value) throws IOException {
    kvPusher.collect(key, value);
  };

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != kvPusher) {
      kvPusher.close();
    }

    if (null != kvWriter) {
      kvWriter.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }

    closed = true;
  }
}