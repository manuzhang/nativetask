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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class HBasePlatform extends Platform {
  private static final Logger LOG = Logger.getLogger(HBasePlatform.class);
  public static final String DEFAULT_NATIVE_LIBRARY = "HBasePlatform=libnativetaskhbase.so";

  public HBasePlatform() {
  }

  @Override
  public void init() throws IOException {
    registerKey("org.apache.hadoop.hbase.io.ImmutableBytesWritable", ImmutableBytesWritableSerializer.class);
    LOG.info("HBase platform inited");
  }

  @Override
  public String name() {
    return "HBase";
  }

  @Override
  public boolean support(String keyClassName, INativeSerializer serializer, JobConf job) {
    if (super.support(keyClassName, serializer, job)) {
      Class comparatorClass = job.getClass(MRJobConfig.KEY_COMPARATOR, null, RawComparator.class);
      if (comparatorClass != null) {
        String message = "Native output collector don't support customized java comparator "
          + comparatorClass.getName();
        LOG.error(message);
			} else {
				String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClassName;
				job.set(nativeComparator, "HBasePlatform.HBasePlatform::ImmutableBytesWritableComparator");
				if (job.get(Constants.NATIVE_CLASS_LIBRARY_BUILDIN) == null) {
					job.set(Constants.NATIVE_CLASS_LIBRARY_BUILDIN, DEFAULT_NATIVE_LIBRARY);
				}
			}
			return true;
		}
		return false;
  }

  public static class ImmutableBytesWritableSerializer implements INativeComparable, INativeSerializer<ImmutableBytesWritable> {

    public ImmutableBytesWritableSerializer() throws ClassNotFoundException, SecurityException, NoSuchMethodException {
    }

    @Override
    public int getLength(ImmutableBytesWritable w) throws IOException {
      return 4 + w.getLength();
    }

    @Override
    public void serialize(ImmutableBytesWritable w, DataOutput out) throws IOException {
      w.write(out);
    }

    @Override
    public void deserialize(DataInput in, int length, ImmutableBytesWritable w ) throws IOException {
      w.readFields(in);
    }
  }
}
