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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

public class ImmutableBytesWritableSerializer implements INativeComparable,
    INativeSerializer<Writable> {
  private Method getLength;
  private Method getOffset;
  private Method getBytes;
  private Class<?> klass;
  private Method set;

  public ImmutableBytesWritableSerializer() throws ClassNotFoundException,
      SecurityException, NoSuchMethodException {
    this.klass = this.getClass().getClassLoader()
        .loadClass("org.apache.hadoop.hbase.io.ImmutableBytesWritable");
    this.getLength = klass.getMethod("getLength");
    this.getOffset = klass.getMethod("getOffset");
    this.getBytes = klass.getMethod("get");
    this.set = klass.getMethod("set", byte[].class);
  }

  @Override
  public int getLength(Writable w) throws IOException {
    try {
      return (Integer) getLength.invoke(w);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void serialize(Writable w, DataOutput out) throws IOException {
    try {
      byte[] b = (byte[]) getBytes.invoke(w);
      int off = (Integer) getOffset.invoke(w);
      int len = (Integer) getLength.invoke(w);
      out.write(b, off, len);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deserialize(Writable w, int length, DataInput in)
      throws IOException {
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    try {
      set.invoke(w, bytes);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}