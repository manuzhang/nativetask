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

import org.apache.hadoop.mapred.nativetask.serde.pig.NullableBagSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableBigDecimalWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableBigIntegerWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableBooleanWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableBytesWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableDateTimeWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableDoubleWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableFloatWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableIntWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableLongWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableTextSerializer;
import org.apache.hadoop.mapred.nativetask.serde.pig.NullableTupleSerializer;

public class PigPlatform extends Platform {

  public PigPlatform() throws IOException {
    
  }
  
  @Override
  public void init() throws IOException {
    registerKey("org.apache.pig.impl.io.NullableBag",
        NullableBagSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableBigDecimalWritable",
        NullableBigDecimalWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableBigIntegerWritable",
        NullableBigIntegerWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableBooleanWritable",
        NullableBooleanWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableBytesWritable",
        NullableBytesWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableDateTimeWritable",
        NullableDateTimeWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableDoubleWritable",
        NullableDoubleWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableFloatWritable",
        NullableFloatWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableIntWritable",
        NullableIntWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableLongWritable",
        NullableLongWritableSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableText",
        NullableTextSerializer.class);
    registerKey("org.apache.pig.impl.io.NullableTuple",
        NullableTupleSerializer.class);
  }

  @Override
  public String name() {
    return "Pig";
  }

}
