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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class HBasePlatform extends Platform {

  public HBasePlatform() throws IOException {
  }

  @Override
  public void init() throws IOException {
    registerKey("org.apache.hadoop.hbase.io.ImmutableBytesWritable", ImmutableBytesWritableSerializer.class);
  }

  @Override
  public String name() {
    return "HBase";
  }

  @Override
  public boolean support(INativeSerializer serializer, JobConf job) {
    if (serializer instanceof INativeComparable) {
      String keyClass = job.getMapOutputKeyClass().getName();
      String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClass;
      job.set(nativeComparator, "BytesComparator");
      return true;
    } else {
      return false;
    }
  }

  private static class ImmutableBytesWritableSerializer implements INativeComparable, INativeSerializer<ImmutableBytesWritable> {

    public ImmutableBytesWritableSerializer() throws ClassNotFoundException, SecurityException, NoSuchMethodException {
    }

    @Override
    public int getLength(ImmutableBytesWritable w) throws IOException {
      return w.getLength();
    }

    @Override
    public void serialize(ImmutableBytesWritable w, DataOutput out) throws IOException {
      out.write(w.get(), w.getOffset(), w.getLength());
    }

    @Override
    public void deserialize(DataInput in, int length, ImmutableBytesWritable w ) throws IOException {
      final byte[] bytes = new byte[length];
      in.readFully(bytes);
      w.set(bytes);
    }
  }
}
