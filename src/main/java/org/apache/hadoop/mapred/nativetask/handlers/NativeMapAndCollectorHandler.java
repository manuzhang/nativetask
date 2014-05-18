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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * Java Record Reader + Native Mapper
 */

public class NativeMapAndCollectorHandler<IK, IV, OK, OV> implements Closeable, CommandDispatcher {

  private static final Log LOG = LogFactory.getLog(NativeMapAndCollectorHandler.class);
  
  
  public static Command GET_OUTPUT_PATH = new Command(100, "GET_OUTPUT_PATH");
  public static Command GET_OUTPUT_INDEX_PATH = new Command(101, "GET_OUTPUT_INDEX_PATH");
  public static Command GET_SPILL_PATH = new Command(102, "GET_SPILL_PATH");
  public static Command GET_COMBINE_HANDLER = new Command(103, "GET_COMBINE_HANDLER");
  
  public static String NAME = "NativeTask.MMapperHandler";
  private final BufferPusher<IK, IV> kvPusher;
  private final INativeHandler nativeHandler;
  private final ICombineHandler combinerHandler;

  private final OutputPathUtil mapOutputFile;
  private int spillNumber = 0;
  private boolean closed = false;
  private final InputBuffer in;

  private final TaskContext context;

  public static <IK, IV, OK, OV> NativeMapAndCollectorHandler<IK, IV, OK, OV> create(TaskContext context) throws IOException {

    final INativeHandler nativeHandler = NativeBatchProcessor.create(NAME, context.getConf(), DataChannel.OUT);
    final BufferPusher<IK, IV> kvPusher = new BufferPusher<IK, IV>(context.getInputKeyClass(),
        context.getInputValueClass(), nativeHandler);

    ICombineHandler combinerHandler = null;

    try {
      final TaskContext combinerContext = context.copyOf();
      combinerContext.setInputKeyClass(context.getOuputKeyClass());
      combinerContext.setInputValueClass(context.getOutputValueClass());
      combinerHandler = CombinerHandler.create(combinerContext);
    } catch (final ClassNotFoundException e) {
      throw new IOException(e);
    }
    
    if (null != combinerHandler) {
      LOG.info("[NativeMapAndCollectorHandler] combiner is not null");
    }

    return new NativeMapAndCollectorHandler<IK, IV, OK, OV>(context, nativeHandler, kvPusher, combinerHandler);
  }

  protected NativeMapAndCollectorHandler(TaskContext context, INativeHandler nativeHandler,
      BufferPusher<IK, IV> kvPusher, ICombineHandler combiner) throws IOException {
    this.kvPusher = kvPusher;
    this.combinerHandler = combiner;
    this.nativeHandler = nativeHandler;
    nativeHandler.setCommandDispatcher(this);
    this.in = nativeHandler.getInputBuffer();
    this.mapOutputFile = new OutputPathUtil();
    this.context = context;
    this.mapOutputFile.setConf(context.getConf());
  }

  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
    
    
    if (null == command) {
      return null;
    }
        
    Path p = null;
    
    if (command.equals(GET_OUTPUT_PATH)) {
      p = mapOutputFile.getOutputFileForWrite(-1);
    } else if (command.equals(GET_OUTPUT_INDEX_PATH)) {
      p = mapOutputFile.getOutputIndexFileForWrite(-1);
    } else if (command.equals(GET_SPILL_PATH)) {
      p = mapOutputFile.getSpillFileForWrite(spillNumber++, -1);
    } else if (command.equals(GET_COMBINE_HANDLER)) {
    
      if (null == combinerHandler) {
        return null;
      }
      final ReadWriteBuffer result = new ReadWriteBuffer(8);
      result.writeLong(combinerHandler.getId());
      return result;
    } else {
      throw new IOException("Illegal command: " + command.toString());
    }
    if (p != null) {
      final ReadWriteBuffer result = new ReadWriteBuffer();
      result.writeString(p.toUri().getPath());
      return result;
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
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

    if (null != combinerHandler) {
      combinerHandler.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }

    closed = true;
  }
}