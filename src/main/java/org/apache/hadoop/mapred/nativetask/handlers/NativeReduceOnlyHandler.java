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
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.NativeDataReader;
import org.apache.hadoop.mapred.nativetask.NativeDataWriter;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * Java record reader + Native reducer + Java record writer.
 * 
 * @param <IK>
 * @param <IV>
 * @param <OK>
 * @param <OV>
 */
public class NativeReduceOnlyHandler<IK, IV, OK, OV> extends
    NativeBatchProcessor<IK, IV, OK, OV> {

  private RawKeyValueIterator rIter;
  private Writable tmpInputKey;
  private Writable tmpInputValue;
  private boolean inputKVBufferd = false;

  Writable tmpOutputKey;
  Writable tmpOutputValue;
  final private RecordWriter<OK, OV> writer;

  enum KVState {
    KEY, VALUE
  }

  KVState state = KVState.KEY;

  public NativeReduceOnlyHandler(int inputBufferCapacity,
      int outputBufferCapacity, Class<IK> iKClass, Class<IV> iVClass,
      Class<OK> oKClass, Class<OV> oVClass, JobConf conf,
      RecordWriter<OK, OV> writer, Progressable progress,
      RawKeyValueIterator rIter) throws IOException {
    super(iKClass, iVClass, oKClass, oVClass, "NativeTask.RReducerHandler",
        inputBufferCapacity, outputBufferCapacity);
    this.rIter = rIter;
    this.writer = writer;
    if (null != oKClass) {
      tmpOutputKey = (Writable) ReflectionUtils.newInstance(oKClass, conf);
    }
    if (null != oVClass) {
      tmpOutputValue = (Writable) ReflectionUtils.newInstance(oVClass, conf);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected boolean flushOutputAndProcess(NativeDataReader reader, int length)
      throws IOException {

    int totalRead = 0;

    while (length > totalRead) {
      final int read = deserializer.deserializeKV(reader, tmpOutputKey,
          tmpOutputValue);
      if (read != 0) {
        totalRead += read;
        writer.write((OK) tmpOutputKey, (OV) tmpOutputValue);
      } else {
        break;
      }
    }
    if (length != totalRead) {
      throw new IOException("We expect to read " + length
          + ", but we actually read: " + totalRead);
    }
    return true;
  }

  /**
   * 
   * @param length
   *          , expect to fill length into input buffer
   * @return, actually filled length
   * @throws IOException
   */
  private int refill(NativeDataWriter out, int length) throws IOException {
    int remain = length;

    if (inputKVBufferd) {
      int written = serializer.serializeKV(out, tmpInputKey, tmpInputValue);
      if (written == 0) {
        return 0;
      }
      remain -= written;
    }

    while (rIter.next() && remain > 0) {
      inputKVBufferd = true;
      tmpInputValue.readFields(rIter.getKey());
      tmpInputKey.readFields(rIter.getValue());
      int written = serializer.serializeKV(out, remain, tmpInputKey,
          tmpInputValue);
      if (written == 0) {
        final int totalWritten = length - remain;
        return totalWritten;
      } else {
        inputKVBufferd = false;
      }
      remain -= written;
    }
    final int totalWritten = length - remain;
    return totalWritten;
  }

  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    // must be refill
    if (data[0] != (byte) 'r') {
      throw new IOException("command not support");
    }
    int length = toInt(data, 1, 4); // load length to read
    return toBytes(refill(getWriter(), length));
  }

  public void run() throws IOException {
    sendCommandToNative(BytesUtil.toBytes("run"));
  }

  private static int toInt(byte[] bytes, int offset, final int length) {
    final int SIZEOF_INT = 4;
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw new RuntimeException(
          "toInt exception. length not equals to SIZE of Int or buffer overflow");
    }
    int n = 0;
    for (int i = (offset + length) - 1; i >= offset; i--) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  private static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    for (int i = 0; i <= 3; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    return b;
  }
}