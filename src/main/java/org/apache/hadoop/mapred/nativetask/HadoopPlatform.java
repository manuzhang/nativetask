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

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.serde.BoolWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.ByteWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.BytesWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.DefaultSerializer;
import org.apache.hadoop.mapred.nativetask.serde.DoubleWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.FloatWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.IntWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.LongWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NullWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.TextSerializer;
import org.apache.hadoop.mapred.nativetask.serde.VIntWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.VLongWritableSerializer;

public class HadoopPlatform extends Platform {

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
  }

  @Override
  public String name() {
    return "Hadoop";
  }
}
