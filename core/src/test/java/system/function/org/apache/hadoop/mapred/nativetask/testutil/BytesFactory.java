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
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;


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
/*    } else if (className.equals(ImmutableBytesWritable.class.getName())) {
      final byte[] bytes = new byte[seed.length];
      System.arraycopy(seed, 0, bytes, 0, seed.length);
      return new ImmutableBytesWritable(bytes);  */
    } else if (className.equals(UTF8.class.getName())) {
      return new UTF8(BytesUtil.toStringBinary(seed));
    } else if (className.equals(MockValueClass.class.getName())) {
      return new MockValueClass(seed);
/*    } else if (TestInputFile.mahoutMap.containsKey(className)) {
      return newMahoutObject(seed, className);
    } else if (TestInputFile.pigMap.containsKey(className)) {
      return newPigObject(seed, className);*/
    } else {
      return null;
    }
  }

/*  private static Object newMahoutObject(byte[] seed, String className) {
    if (className.equals(VarIntWritable.class.getName())) {
      return new VarIntWritable(Bytes.toInt(seed));
    } else if (className.equals(VarLongWritable.class.getName())) {
      return new VarLongWritable(Bytes.toLong(seed));
    } else if (className.equals(TreeID.class.getName())) {
      TreeID treeID = new TreeID();
      treeID.set(Bytes.toLong(seed));
      return treeID;
    } else if (className.equals(SplitPartitionedWritable.class.getName())) {
      SplitPartitionedWritable spWritable = new SplitPartitionedWritable();
      long taskItemOrdinal = Math.abs(Bytes.toLong(seed, 4));
      spWritable.setTaskItemOrdinal(taskItemOrdinal);
      return spWritable;
    } else if (className.equals(EntityEntityWritable.class.getName())) {
      EntityEntityWritable entityWritable = new EntityEntityWritable(
        Bytes.toLong(seed, 0), Bytes.toLong(seed, 8));
      return entityWritable;
    } else if (className.equals(Gram.class.getName())) {
      String ngram = Bytes.toStringBinary(seed);
      return new Gram(ngram, Gram.Type.NGRAM);
    } else if (className.equals(GramKey.class.getName())) {
      int primaryLength = r.nextInt(seed.length);
      Gram gram = new Gram(Bytes.toStringBinary(seed, 0,
        Math.max(primaryLength, 1)), Gram.Type.NGRAM);
      byte[] order = new byte[seed.length - primaryLength];
      System.arraycopy(seed, primaryLength, order, 0, order.length);
      return new GramKey(gram, order);
    } else if (className.equals(StringTuple.class.getName())) {
      int tupleSize = r.nextInt(4);
      StringTuple stringTuple = new StringTuple();
      for (int i = 0; i < tupleSize; i++) {
        int index = r.nextInt(seed.length);
        stringTuple.add(Bytes.toStringBinary(seed, index, seed.length - index));
      }
      return stringTuple;
    } else {
      return null;
    }
  }

  private static Object newPigObject(byte[] seed, String className) {
    PigNullableWritable pigNullableWritable = null;
    if (seed.length == TestInputFile.pigMap.get(className).minBytesNum) {
      try {
        Class<?> clazz = Class.forName(className);
        pigNullableWritable = (PigNullableWritable) clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException("init class " + className + " failed");
      }
      pigNullableWritable.setNull(true);
      return pigNullableWritable;
    } else if (className.equals(NullableBooleanWritable.class.getName())) {
      pigNullableWritable = new NullableBooleanWritable(seed[0] % 2 == 1 ? true : false);
    } else if (className.equals(NullableDateTimeWritable.class.getName())) {
      pigNullableWritable = new NullableDateTimeWritable(new DateTime(Bytes.toLong(seed)));
    } else if (className.equals(NullableDoubleWritable.class.getName())) {
      pigNullableWritable = new NullableDoubleWritable(r.nextDouble());
    } else if (className.equals(NullableFloatWritable.class.getName())) {
      pigNullableWritable = new NullableFloatWritable(r.nextFloat());
    } else if (className.equals(NullableIntWritable.class.getName())) {
      pigNullableWritable = new NullableIntWritable(Bytes.toInt(seed));
    } else if (className.equals(NullableLongWritable.class.getName())) {
      pigNullableWritable = new NullableLongWritable(Bytes.toLong(seed));
    } else if (className.equals(NullableBytesWritable.class.getName())) {
      String string = Bytes.toStringBinary(seed, 0, seed.length - 2);
      pigNullableWritable = new NullableBytesWritable(string);
    } else if (className.equals(NullableText.class.getName())) {
      pigNullableWritable = new NullableText(Bytes.toStringBinary(seed, 0, seed.length - 2));
    } else if (className.equals(NullableTuple.class.getName())) {
      pigNullableWritable = new NullableTuple(createPigTuple(seed));
    }
    pigNullableWritable.setIndex((byte) (seed[seed.length - 1] & 0x7F));
    return pigNullableWritable;
  }

  private static Tuple createPigTuple(byte[] seed) {
    BinSedesTupleFactory factory = new BinSedesTupleFactory();
    Tuple tuple = factory.newTuple();
    tuple.append(new Boolean(seed[0] % 2 == 1 ? true : false));
    tuple.append(Bytes.toStringBinary(seed));

*//*    if (seed.length >= 4) {
      tuple.append(new Integer(Bytes.toInt(seed)));
    //  tuple.append(r.nextFloat());
    }
    if (seed.length >= 8) {
      tuple.append(new Long(Bytes.toLong(seed)));
    //  tuple.append(r.nextDouble());
    //  tuple.append(new DateTime(Bytes.toLong(seed)));
    } *//*
    return tuple;
  }*/

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
/*    } else if (className.equals(ImmutableBytesWritable.class.getName())) {
      return ((ImmutableBytesWritable) obj).get();
    } else if (className.equals(UTF8.class.getName())) {
      return ((UTF8) obj).getBytes();
    } else if (TestInputFile.mahoutMap.containsKey(className)) {
      return MahoutObjectToBytes(obj);
    } else if (TestInputFile.pigMap.containsKey(className)) {
      return PigObjectToBytes(obj);*/
    } else {
      return new byte[0];
    }
  }

/*  private static <VTYPE> byte[] MahoutObjectToBytes(VTYPE obj) {
    String className = obj.getClass().getName();
    if (className.equals(VarIntWritable.class.getName())) {
      return Bytes.toBytes(((VarIntWritable) obj).get());
    } else if (className.equals(VarLongWritable.class.getName())) {
      return Bytes.toBytes(((VarLongWritable) obj).get());
    } else if (className.equals(TreeID.class.getName())) {
      return Bytes.toBytes(((TreeID) obj).get());
    } else if (className.equals(SplitPartitionedWritable.class.getName())) {
      byte[] bytes = new byte[12];
      SplitPartitionedWritable spWritable = (SplitPartitionedWritable) obj;
      System.arraycopy(Bytes.toBytes(spWritable.getTaskId()), 0, bytes, 0, 4);
      System.arraycopy(Bytes.toBytes(spWritable.getTaskItemOrdinal()), 0,
        bytes, 4, 8);
      return bytes;
    } else if (className.equals(EntityEntityWritable.class.getName())) {
      byte[] bytes = new byte[16];
      String[] nums = ((EntityEntityWritable) obj).toString().split("\t");
      System.arraycopy(Bytes.toBytes(Long.parseLong(nums[0])), 0, bytes, 0, 8);
      System.arraycopy(Bytes.toBytes(Long.parseLong(nums[1])), 0, bytes, 8, 8);
      return bytes;
    } else if (className.equals(Gram.class.getName())) {
      return ((Gram) obj).getBytes();
    } else if (className.equals(GramKey.class.getName())) {
      return ((GramKey) obj).getBytes();
    } else if (className.equals(StringTuple.class.getName())) {
      return Bytes.toBytes(((StringTuple) obj).toString());
    } else {
      return new byte[0];
    }
  }

  private static <VTYPE> byte[] PigObjectToBytes(VTYPE obj) {
    String className = obj.getClass().getName();
    if (className.equals(NullableBooleanWritable.class.getName())) {
      Boolean bool = (Boolean) ((NullableBooleanWritable) obj).getValueAsPigType();
      return Bytes.toBytes(bool);
    } else if (className.equals(NullableDateTimeWritable.class.getName())) {
      DateTime dateTime = (DateTime) ((NullableDateTimeWritable) obj).getValueAsPigType();
      return Bytes.toBytes(dateTime.getMillis());
    } else if (className.equals(NullableDoubleWritable.class.getName())) {
      Double num = (Double) ((NullableDoubleWritable) obj).getValueAsPigType();
      return Bytes.toBytes(num);
    } else if (className.equals(NullableFloatWritable.class.getName())) {
      Float num = (Float) ((NullableFloatWritable) obj).getValueAsPigType();
      return Bytes.toBytes(num);
    } else if (className.equals(NullableLongWritable.class.getName())) {
      Long num = (Long) ((NullableLongWritable) obj).getValueAsPigType();
      return Bytes.toBytes(num);
    } else if (className.equals(NullableIntWritable.class.getName())) {
      Integer num = (Integer) ((NullableIntWritable) obj).getValueAsPigType();
      return Bytes.toBytes(num);
    } else if (className.equals(NullableBytesWritable.class.getName())) {
      String string = (String) ((NullableBytesWritable) obj).getValueAsPigType();
      return Bytes.toBytes(string);
    } else if (className.equals(NullableText.class.getName())) {
      String string = (String) ((NullableText) obj).getValueAsPigType();
      return Bytes.toBytes(string);
    } else if (className.equals(NullableTuple.class.getName())) {
      DefaultTuple tuple = (DefaultTuple) ((NullableTuple) obj).getValueAsPigType();
      return Bytes.toBytes(tuple.toString());
    }
    return new byte[0];
  }*/
}
