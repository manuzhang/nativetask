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
package org.apache.hadoop.mapred.nativetask.testutil;

import java.util.Random;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;
import org.apache.mahout.classifier.df.mapreduce.partial.TreeID;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.hadoop.stochasticsvd.SplitPartitionedWritable;
import org.apache.mahout.vectorizer.collocations.llr.Gram;
import org.apache.mahout.vectorizer.collocations.llr.GramKey;

public class BytesFactory {
  public static Random r = new Random();

  public static Object newObject(byte[] seed, String className) {
    r.setSeed(seed.hashCode());
    if (className.equals(IntWritable.class.getName())) {
      return new IntWritable(BytesUtil.toInt(seed));
    } else if (className.equals(FloatWritable.class.getName())) {
      return new FloatWritable(r.nextFloat());
    } else if (className.equals(DoubleWritable.class.getName())) {
      return new DoubleWritable(r.nextDouble());
    } else if (className.equals(LongWritable.class.getName())) {
      return new LongWritable(BytesUtil.toLong(seed));
    } else if (className.equals(VIntWritable.class.getName())) {
      return new VIntWritable(BytesUtil.toInt(seed));
    } else if (className.equals(VLongWritable.class.getName())) {
      return new VLongWritable(BytesUtil.toLong(seed));
    } else if (className.equals(BooleanWritable.class.getName())) {
      return new BooleanWritable(seed[0] % 2 == 1 ? true : false);
    } else if (className.equals(Text.class.getName())) {
      return new Text(BytesUtil.toStringBinary(seed));
    } else if (className.equals(ByteWritable.class.getName())) {
      return new ByteWritable(seed.length > 0 ? seed[0] : 0);
    } else if (className.equals(BytesWritable.class.getName())) {
      return new BytesWritable(seed);
    } else if (className.equals(UTF8.class.getName())) {
      return new UTF8(BytesUtil.toStringBinary(seed));
    } else if (className.equals(MockValueClass.class.getName())) {
      return new MockValueClass(seed);
    }  else if (TestInputFile.mahoutMap.containsKey(className)) {
      return newMahoutObject(seed, className);
    } else {
      return null;
    }
  }

  private static Object newMahoutObject(byte[] seed, String className) {
    if (className.equals(VarIntWritable.class.getName())) {
      return new VarIntWritable(BytesUtil.toInt(seed));
    } else if (className.equals(VarLongWritable.class.getName())) {
      return new VarLongWritable(BytesUtil.toLong(seed));
    } else if (className.equals(TreeID.class.getName())) {
      TreeID treeID = new TreeID();
      treeID.set(BytesUtil.toLong(seed));
      return treeID;
    } else if (className.equals(SplitPartitionedWritable.class.getName())) {
      SplitPartitionedWritable spWritable = new SplitPartitionedWritable();
      long taskItemOrdinal = Math.abs(BytesUtil.toLong(seed, 4));
      spWritable.setTaskItemOrdinal(taskItemOrdinal);
      return spWritable;
    } else if (className.equals(EntityEntityWritable.class.getName())) {
      EntityEntityWritable entityWritable = new EntityEntityWritable(
          BytesUtil.toLong(seed, 0), BytesUtil.toLong(seed, 8));
      return entityWritable;
    } else if (className.equals(Gram.class.getName())) {
      String ngram = BytesUtil.toStringBinary(seed);
      return new Gram(ngram, Gram.Type.NGRAM);
    } else if (className.equals(GramKey.class.getName())) {
      int primaryLength = r.nextInt(seed.length);
      Gram gram = new Gram(BytesUtil.toStringBinary(seed, 0,
          Math.max(primaryLength, 1)), Gram.Type.NGRAM);
      byte[] order = new byte[seed.length - primaryLength];
      System.arraycopy(seed, primaryLength, order, 0, order.length);
      return new GramKey(gram, order);
    } else if (className.equals(StringTuple.class.getName())) {
      int tupleSize = r.nextInt(4);
      StringTuple stringTuple = new StringTuple();
      for (int i = 0; i < tupleSize; i++) {
        int index = r.nextInt(seed.length);
        stringTuple.add(BytesUtil.toStringBinary(seed, index, seed.length - index));
      }
      return stringTuple;
    } else {
      return null;
    }
  }
 
  public static <VTYPE> byte[] fromBytes(byte[] bytes) throws Exception {
    throw new Exception("Not supported");
  }

  public static <VTYPE> byte[] toBytes(VTYPE obj) {
    final String className = obj.getClass().getName();
    if (className.equals(IntWritable.class.getName())) {
      return BytesUtil.toBytes(((IntWritable) obj).get());
    } else if (className.equals(FloatWritable.class.getName())) {
      return BytesUtil.toBytes(((FloatWritable) obj).get());
    } else if (className.equals(DoubleWritable.class.getName())) {
      return BytesUtil.toBytes(((DoubleWritable) obj).get());
    } else if (className.equals(LongWritable.class.getName())) {
      return BytesUtil.toBytes(((LongWritable) obj).get());
    } else if (className.equals(VIntWritable.class.getName())) {
      return BytesUtil.toBytes(((VIntWritable) obj).get());
    } else if (className.equals(VLongWritable.class.getName())) {
      return BytesUtil.toBytes(((VLongWritable) obj).get());
    } else if (className.equals(BooleanWritable.class.getName())) {
      return BytesUtil.toBytes(((BooleanWritable) obj).get());
    } else if (className.equals(Text.class.getName())) {
      return BytesUtil.toBytes(((Text) obj).toString());
    } else if (className.equals(ByteWritable.class.getName())) {
      return BytesUtil.toBytes(((ByteWritable) obj).get());
    } else if (className.equals(BytesWritable.class.getName())) {
      return ((BytesWritable) obj).getBytes();
    } else {
      return new byte[0];
    }
  }
}
