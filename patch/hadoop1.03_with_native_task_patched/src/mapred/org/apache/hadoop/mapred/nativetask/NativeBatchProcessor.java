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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;

public abstract class NativeBatchProcessor<IK, IV, OK, OV> implements
    INativeHandler {

  private static Log LOG = LogFactory.getLog(NativeBatchProcessor.class);

  protected ByteBuffer inputBuffer;
  private ByteBuffer outputBuffer;

  private String nativeHandlerName;
  private long nativeHandlerAddr;
  private boolean isInputFinished = false;

  protected KVSerializer<IK, IV> serializer;
  protected KVSerializer<OK, OV> deserializer;

  protected NativeDataReader nativeReader;
  protected NativeDataWriter nativeWriter;

  static {
    // InitIDs();
    if (NativeRuntime.isNativeLibraryLoaded()) {
      InitIDs();
    }
  }

  public NativeBatchProcessor(Class<IK> iKClass, Class<IV> iVClass,
      Class<OK> oKClass, Class<OV> oVClass, String nativeHandlerName,
      int inputBufferCapacity, int outputBufferCapacity) throws IOException {
    if (inputBufferCapacity > 0) {
      this.inputBuffer = ByteBuffer.allocateDirect(inputBufferCapacity);
      this.inputBuffer.order(ByteOrder.BIG_ENDIAN);
    }
    if (outputBufferCapacity > 0) {
      this.outputBuffer = ByteBuffer.allocateDirect(outputBufferCapacity);
      this.outputBuffer.order(ByteOrder.BIG_ENDIAN);
    }
    this.nativeHandlerName = nativeHandlerName;

    this.nativeReader = new NativeInputStream(outputBuffer);
    this.nativeWriter = new NativeOutputStream(inputBuffer) {

      @Override
      public void flush() throws IOException {
        flushInputAndProcess();
      }

      @Override
      public void close() throws IOException {
        finishInput();
      }

    };

    if (null != iKClass && null != iVClass) {
      this.serializer = new KVSerializer<IK, IV>(iKClass, iVClass);
    }
    if (null != oKClass && null != oVClass) {
      this.deserializer = new KVSerializer<OK, OV>(oKClass, oVClass);
    }
  }

  @Override
  public void init(Configuration conf) throws IOException {
    this.nativeHandlerAddr = NativeRuntime
        .createNativeObject(nativeHandlerName);
    if (this.nativeHandlerAddr == 0) {
      throw new RuntimeException("Native object create failed, class: "
          + nativeHandlerName);
    }
    setupHandler(nativeHandlerAddr);
  }

  @Override
  public synchronized void close() throws IOException {
    nativeWriter.close();
    if (nativeHandlerAddr != 0) {
      NativeRuntime.releaseNativeObject(nativeHandlerAddr);
      nativeHandlerAddr = 0;
    }
  }

  private void finishInput() throws IOException {
    if (null == inputBuffer) {
      return;
    }

    if (isInputFinished) {
      return;
    }
    if (inputBuffer.position() > 0) {
      flushInputAndProcess();
    }
    nativeFinish(nativeHandlerAddr);
    isInputFinished = true;
  }

  private void flushInputAndProcess() throws IOException {
    nativeProcessInput(nativeHandlerAddr, inputBuffer.position());
    inputBuffer.position(0);
  }

  protected byte[] sendCommandToNative(byte[] cmd) throws IOException {
    return nativeCommand(nativeHandlerAddr, cmd);
  }

  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    return null;
  }

  /**
   * Called by native side, clean output buffer so native side can continue
   * processing
   */
  private void flushOutput(int length) throws IOException {
    outputBuffer.position(0);
    outputBuffer.limit(length);
    flushOutputAndProcess(nativeReader, length);
  }

  /**
   * Cache JNI field & method ids
   */
  private static native void InitIDs();

  /**
   * Setup native side BatchHandler
   */
  private native void setupHandler(long nativeHandlerAddr);

  /**
   * Let native side to process data in inputBuffer
   * 
   * @param handler
   * @param length
   */
  private native void nativeProcessInput(long handler, int length);

  /**
   * Notice native side input is finished
   * 
   * @param handler
   */
  private native void nativeFinish(long handler);

  /**
   * Send control message to native side
   * 
   * @param cmd
   *          command data
   * @return return value
   */
  private native byte[] nativeCommand(long handler, byte[] cmd);

  @Override
  public NativeDataWriter getWriter() {
    return nativeWriter;
  }

  @Override
  public NativeDataReader getReader() {
    return nativeReader;
  }

  protected void finishOutput() {
  }

  /**
   * @param outputbuffer
   * @return true if succeed
   */
  protected boolean flushOutputAndProcess(NativeDataReader outputbuffer,
      int length) throws IOException {
    return true;
  }
}
