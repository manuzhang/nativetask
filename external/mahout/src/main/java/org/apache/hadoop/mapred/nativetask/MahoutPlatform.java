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

import org.apache.hadoop.mapred.nativetask.serde.LongWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.EntityEntityWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.GramKeySerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.GramSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.SplitPartitionedWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.StringTupleSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.VarIntWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.mahout.VarLongWritableSerializer;

public class MahoutPlatform extends Platform{
	
  public MahoutPlatform() throws IOException {

  }

  @Override
  public void init() throws IOException {
    // TODO Auto-generated method stub
    registerKey("org.apache.mahout.classifier.df.mapreduce.partial.TreeID",
        LongWritableSerializer.class);
    registerKey("org.apache.mahout.common.StringTuple",
        StringTupleSerializer.class);
    registerKey("org.apache.mahout.vectorizer.collocations.llr.Gram",
        GramSerializer.class);
    registerKey("org.apache.mahout.vectorizer.collocations.llr.GramKey",
        GramKeySerializer.class);
    registerKey("org.apache.mahout.math.hadoop.stochasticsvd.SplitPartitionedWritable",
        SplitPartitionedWritableSerializer.class);
    registerKey("org.apache.mahout.cf.taste.hadoop.EntityEntityWritable",
        EntityEntityWritableSerializer.class);
    registerKey("org.apache.mahout.math.VarIntWritable",
        VarIntWritableSerializer.class);
    registerKey("org.apache.mahout.math.VarLongWritable",
        VarLongWritableSerializer.class);
  }

  @Override
  public String name() {
    // TODO Auto-generated method stub
    return "Mahout";
  }

}
