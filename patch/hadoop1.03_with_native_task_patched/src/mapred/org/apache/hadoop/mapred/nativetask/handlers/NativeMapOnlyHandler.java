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
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.NativeDataReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * Java Record Reader + Native Mapper + Java Collector Useful for map-only job,
 * since we don't need sorting.
 * 
 */
public class NativeMapOnlyHandler<IK, IV, OK, OV> extends
    NativeBatchProcessor<IK, IV, OK, OV> {

  enum KVState {
    KEY, VALUE
  }

  RecordWriter<Writable, Writable> writer;
  Writable tmpKey;
  Writable tmpValue;

  KVState state = KVState.KEY;

  @SuppressWarnings("unchecked")
  public NativeMapOnlyHandler(int inputBufferCapacity,
      int outputBufferCapacity, Class<IK> iKClass, Class<IV> iVClass,
      Class<OK> oKClass, Class<OV> oVClass, JobConf conf,
      RecordWriter<OK, OV> writer) throws IOException {
    super(iKClass, iVClass, oKClass, oVClass, "NativeTask.MMapperHandler",
        inputBufferCapacity, outputBufferCapacity);

    this.writer = (RecordWriter<Writable, Writable>) writer;
    tmpKey = (Writable) ReflectionUtils.newInstance(oKClass, conf);
    tmpValue = (Writable) ReflectionUtils.newInstance(oVClass, conf);
  }

  @Override
  protected boolean flushOutputAndProcess(NativeDataReader dataReader,
      int length) throws IOException {

    int totalRead = 0;

    while (length > totalRead) {
      final int read = deserializer.deserializeKV(dataReader, tmpKey, tmpValue);
      if (read != 0) {
        totalRead += read;
        writer.write(tmpKey, tmpValue);
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

  public void process(IK key, IV value) throws IOException {
    serializer.serializeKV(nativeWriter, (Writable) key, (Writable) value);
  };

  @Override
  public void close() throws IOException {
    super.close();
  }
}