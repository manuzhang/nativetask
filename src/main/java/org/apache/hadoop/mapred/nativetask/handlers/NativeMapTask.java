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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.util.OutputPathUtil;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * 
 * Full Native Map Task
 */
public class NativeMapTask implements CommandDispatcher, Closeable {
  public static String NAME = "NativeTask.MMapTaskHandler";
  private static final Log LOG = LogFactory.getLog(NativeMapTask.class);
  public static Command GET_OUTPUT_PATH = new Command(100, "GET_OUTPUT_PATH");
  public static Command GET_OUTPUT_INDEX_PATH = new Command(101, "GET_OUTPUT_INDEX_PATH");
  public static Command GET_SPILL_PATH = new Command(102, "GET_SPILL_PATH");

  public static Command RUN = new Command(3, "RUN");

  private final OutputPathUtil mapOutputFile;
  private int spillNumber = 0;
  private final INativeHandler nativeHandler;
  private boolean closed = false;

  public static NativeMapTask create(Configuration conf, TaskAttemptID taskAttemptID) throws IOException {
    final INativeHandler nativeHandler = NativeBatchProcessor.create(NAME, conf, DataChannel.NONE);
    return new NativeMapTask(conf, nativeHandler);
  }

  public NativeMapTask(Configuration conf, INativeHandler nativeHandler) throws IOException {
    this.mapOutputFile = new OutputPathUtil();
    this.mapOutputFile.setConf(conf);
    this.nativeHandler = nativeHandler;
  }

  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
    Path p = null;
 
    if (null == command) {
      return null;
    }
    
    if (command.equals(GET_OUTPUT_PATH)) {
      p = mapOutputFile.getOutputFileForWrite(-1);
    } else if (command.equals(GET_OUTPUT_INDEX_PATH)) {
      p = mapOutputFile.getOutputIndexFileForWrite(-1);
    } else if (command.equals(GET_SPILL_PATH)) {
      p = mapOutputFile.getSpillFileForWrite(spillNumber++, -1);
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

  public void run() throws IOException {
    nativeHandler.call(RUN, null);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    nativeHandler.close();
    closed = true;
  }
}