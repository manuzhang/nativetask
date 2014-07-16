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

import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.*;
import org.joda.time.DateTime;

import java.util.Random;

public class BytesFactory {
  public static Random r = new Random();

  public static Object newObject(byte[] seed, String className) {
    r.setSeed(seed.hashCode());
    if (TestInputFile.pigMap.containsKey(className)){
      return newPigObject(seed, className);
    } else {
      return null;
    }
  }

  private static Object newPigObject(byte[] seed, String className) {
    PigNullableWritable pigNullableWritable = null;
    if (seed.length == TestInputFile.pigMap.get(className).minBytesNum) {
      try {
        Class<?> clazz = Class.forName(className);
        pigNullableWritable = (PigNullableWritable)clazz.newInstance();
      } catch (Exception e) {
        throw new RuntimeException("init class " + className + " failed");
      }
      pigNullableWritable.setNull(true);
      return pigNullableWritable;
    } else if (className.equals(NullableBooleanWritable.class.getName())) {
      pigNullableWritable = new NullableBooleanWritable(seed[0] % 2 == 1 ? true : false);
    } else if (className.equals(NullableDateTimeWritable.class.getName())) {
      pigNullableWritable = new NullableDateTimeWritable(new DateTime(BytesUtil.toLong(seed)));
    } else if (className.equals(NullableDoubleWritable.class.getName())) {
      pigNullableWritable = new NullableDoubleWritable(r.nextDouble());
    } else if (className.equals(NullableFloatWritable.class.getName())) {
      pigNullableWritable = new NullableFloatWritable(r.nextFloat());
    } else if (className.equals(NullableIntWritable.class.getName())) {
      pigNullableWritable = new NullableIntWritable(BytesUtil.toInt(seed));
    } else if (className.equals(NullableLongWritable.class.getName())) {
      pigNullableWritable = new NullableLongWritable(BytesUtil.toLong(seed));
    } else if (className.equals(NullableBytesWritable.class.getName())) {
      String string = BytesUtil.toStringBinary(seed, 0, seed.length - 2);
      pigNullableWritable = new NullableBytesWritable(string);
    } else if (className.equals(NullableText.class.getName())) {
      pigNullableWritable = new NullableText(BytesUtil.toStringBinary(seed, 0, seed.length - 2));
    } else if (className.equals(NullableTuple.class.getName())) {
      pigNullableWritable = new NullableTuple(createPigTuple(seed));
    } 
    pigNullableWritable.setIndex((byte) (seed[seed.length-1]&0x7F));
    return pigNullableWritable;
  }
  
  private static Tuple createPigTuple(byte[] seed) {
    BinSedesTupleFactory factory = new BinSedesTupleFactory();
    Tuple tuple = factory.newTuple();
    tuple.append(new Boolean(seed[0] % 2 == 1 ? true : false));
    tuple.append(BytesUtil.toStringBinary(seed));

/*    if (seed.length >= 4) {
      tuple.append(new Integer(Bytes.toInt(seed)));
    //  tuple.append(r.nextFloat());
    }
    if (seed.length >= 8) {
      tuple.append(new Long(Bytes.toLong(seed)));
    //  tuple.append(r.nextDouble());
    //  tuple.append(new DateTime(Bytes.toLong(seed)));
    } */
    return tuple;
  }
  
  public static <VTYPE> byte[] fromBytes(byte[] bytes) throws Exception {
    throw new Exception("Not supported");
  }

  public static <VTYPE> byte[] toBytes(VTYPE obj) {
    final String className = obj.getClass().getName();
    if (TestInputFile.pigMap.containsKey(className)){
    	return PigObjectToBytes(obj);
    } else {
      return new byte[0];
    }
  }
  
  private static <VTYPE> byte[] PigObjectToBytes(VTYPE obj) {
    String className = obj.getClass().getName();
    if (className.equals(NullableBooleanWritable.class.getName())) {
      Boolean bool = (Boolean) ((NullableBooleanWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(bool);
    } else if (className.equals(NullableDateTimeWritable.class.getName())) {
      DateTime dateTime = (DateTime) ((NullableDateTimeWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(dateTime.getMillis());
    } else if (className.equals(NullableDoubleWritable.class.getName())) {
      Double num = (Double) ((NullableDoubleWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(num);
    } else if (className.equals(NullableFloatWritable.class.getName())) {
      Float num = (Float) ((NullableFloatWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(num);
    } else if (className.equals(NullableLongWritable.class.getName())) {
      Long num = (Long) ((NullableLongWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(num);
    } else if (className.equals(NullableIntWritable.class.getName())) {
      Integer num = (Integer) ((NullableIntWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(num);
    } else if (className.equals(NullableBytesWritable.class.getName())) {
      String string = (String) ((NullableBytesWritable) obj).getValueAsPigType();
      return BytesUtil.toBytes(string);
    } else if (className.equals(NullableText.class.getName())) {
      String string = (String) ((NullableText) obj).getValueAsPigType();
      return BytesUtil.toBytes(string);
    } else if (className.equals(NullableTuple.class.getName())) {
      DefaultTuple tuple = (DefaultTuple) ((NullableTuple) obj).getValueAsPigType();
      return BytesUtil.toBytes(tuple.toString());
    } 
    return new byte[0];
  }
}
