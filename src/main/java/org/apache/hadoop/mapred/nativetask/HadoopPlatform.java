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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.*;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;

public class HadoopPlatform extends Platform {
  private static final Logger LOG = Logger.getLogger(HadoopPlatform.class);

  public HadoopPlatform() throws IOException {
  }

  @Override
  public void init() throws IOException {
    registerKey(NullWritable.class.getName(), NullWritableSerializer.class);
    registerKey(Text.class.getName(), TextSerializer.class);
    registerKey(LongWritable.class.getName(), LongWritableSerializer.class);
    registerKey(IntWritable.class.getName(), IntWritableSerializer.class);
    registerKey(Writable.class.getName(), DefaultSerializer.class);
    registerKey(BytesWritable.class.getName(), BytesWritableSerializer.class);
    registerKey(BooleanWritable.class.getName(), BoolWritableSerializer.class);
    registerKey(ByteWritable.class.getName(), ByteWritableSerializer.class);
    registerKey(FloatWritable.class.getName(), FloatWritableSerializer.class);
    registerKey(DoubleWritable.class.getName(), DoubleWritableSerializer.class);
    registerKey(VIntWritable.class.getName(), VIntWritableSerializer.class);
    registerKey(VLongWritable.class.getName(), VLongWritableSerializer.class);

    LOG.info("Hadoop platform inited");
  }

  public boolean support(String keyClassName, INativeSerializer serializer, JobConf job) {
    if (super.support(keyClassName, serializer, job)) {
      Class comparatorClass = job.getClass(MRJobConfig.KEY_COMPARATOR, null, RawComparator.class);
      if (comparatorClass != null) {
        String message = "Native output collector don't support customized java comparator "
          + comparatorClass.getName();
        LOG.error(message);
      } else {
        return true;
      }
    }

    return false;
  }


  @Override
  public String name() {
    return "Hadoop";
  }
}
