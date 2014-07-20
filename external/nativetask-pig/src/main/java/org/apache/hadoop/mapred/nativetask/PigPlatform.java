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

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.*;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;


import static org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.*;

public class PigPlatform extends Platform {

  private static Logger LOG = Logger.getLogger(PigPlatform.class);
  public static final String PIG_USER_COMPARATOR = "pig.usercomparator";
  public static final String PIG_SORT_ORDER = "pig.sortOrder";
  public static final String NATIVE_PIG_SORT = "native.pig.sortOrder";
  public static final String PIG_SEC_SORT_ORDER = "pig.secondarySortOrder";
  public static final String NATIVE_PIG_SEC_SORT = "native.pig.secondarySortOrder";
  public static final String NATIVE_PIG_USE_SEC_KEY = "native.pig.useSecondaryKey";
  public static final String DEFAULT_NATIVE_LIBRARY = "PigPlatform=libnativetaskpig.so";

  private Map<String, String> keyClassToNativeComparator = new HashMap<String, String>();
  private Set<String> rawComparatorClass = new HashSet<String>();

  public PigPlatform() {
  }
  
  @Override
  public void init() throws IOException {
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

    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableBooleanWritable",
        "PigNullableBooleanComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableBytesWritable",
        "PigNullableBytesComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableDateTimeWritable",
        "PigNullableDateTimeComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableDoubleWritable",
        "PigNullableDoubleComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableFloatWritable",
        "PigNullableFloatComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableIntWritable",
        "PigNullableIntComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableLongWritable",
        "PigNullableLongComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableText",
        "PigNullableTextComparator");
    keyClassToNativeComparator.put("org.apache.pig.impl.io.NullableTuple",
        "PigNullableTupleComparator");

    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBooleanRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigIntRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigLongRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFloatRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDoubleRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDateTimeRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBytesRawComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleSortComparator");
    rawComparatorClass.add(
        "org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSecondaryKeyComparator");

    LOG.info("Pig platform inited");
  }

  @Override
  public String name() {
    return "Pig";
  }

  @Override
  public boolean support(String keyClassName, INativeSerializer serializer, JobConf job) {
    boolean supported = false;
    if (super.support(keyClassName, serializer, job)) {
      String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClassName;
      Class comparatorClass = job.getClass(MRJobConfig.KEY_COMPARATOR, null);

      if (PigWritableComparator.class.isAssignableFrom(comparatorClass)) {
        job.set(nativeComparator, "PigPlatform.NativeObjectFactory::BytesComparator");
        LOG.info("Pig key types: group only");
        supported = true;
      } else if (!keyClassName.equals("org.apache.pig.impl.io.NullableBigDecimalWritable") &&
				// don't support native comparators of BigDecimalWritable and BigIntegerWritable
        !keyClassName.equals("org.apache.pig.impl.io.NullableBigIntegerWritable") &&
        // don't support user defined comparator
        !job.getBoolean(PIG_USER_COMPARATOR, false)) {
        try {
          if (job.get(PIG_SORT_ORDER, null) != null) {
            boolean[] order = (boolean[]) ObjectSerializer.deserialize(job.get(PIG_SORT_ORDER));
            job.set(NATIVE_PIG_SORT, ConfigUtil.booleansToString(order));
            job.set(nativeComparator, "PigPlatform.PigPlatform::" + keyClassToNativeComparator.get(keyClassName));
            LOG.info("Pig key types: set sort order");
          }
          if (job.get(PIG_SEC_SORT_ORDER, null) != null) {
            boolean[] order = (boolean[]) ObjectSerializer.deserialize(job
              .get(PIG_SEC_SORT_ORDER));
            job.set(NATIVE_PIG_SEC_SORT, ConfigUtil.booleansToString(order));
            job.setBoolean(NATIVE_PIG_USE_SEC_KEY, true);
            job.set(nativeComparator, "PigPlatform.PigPlatform::PigSecondaryKeyComparator");
            LOG.info("Pig key types: set secondary sort order");
          }
          supported = true;
        } catch (final IOException e) {
          LOG.error("cannot deserialize Pig configurations", e);
        }
      }
    }
    if (supported) {
      if (job.get(Constants.NATIVE_CLASS_LIBRARY_BUILDIN) == null) {
        job.set(Constants.NATIVE_CLASS_LIBRARY_BUILDIN, DEFAULT_NATIVE_LIBRARY);
      }
    }
    return supported;
  }

  public static class NullableWritableSerializer implements
    INativeSerializer<PigNullableWritable> {

    public ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    public DataOutputStream outData = new DataOutputStream(outBuffer);
    public int bufferedLength = -1;

    @Override
    public int getLength(PigNullableWritable w) throws IOException {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        bufferedLength = -1;
        outBuffer.reset();

        w.write(outData);
        bufferedLength = outBuffer.size();


        return bufferedLength;
      }
    }

    @Override
    public void serialize(PigNullableWritable w, DataOutput out)
      throws IOException {
      w.write(out);
    }

    @Override
    public void deserialize(DataInput in, int length, PigNullableWritable w)
      throws IOException {
      w.readFields(in);
    }

  }

  public static class NullableTupleSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  public static class NullableTextSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  public static class NullableLongWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {

    @Override
    public int getLength(PigNullableWritable w) throws IOException {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + mValue (long) + mIndex
        return 10;
      }
    }
  }

  public static class NullableIntWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {

    @Override
    public int getLength(PigNullableWritable w) throws IOException {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + mValue (int) + mIndex
        return 6;
      }
    }
  }

  public static class NullableFloatWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {

    @Override
    public int getLength(PigNullableWritable w) throws IOException {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + mValue (float) + mIndex
        return 6;
      }
    }
  }

  public static class NullableDoubleWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {

    @Override
    public int getLength(PigNullableWritable w) throws IOException {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + mValue (double) + mIndex
        return 10;
      }
    }
  }

  public static class NullableDateTimeWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {
    @Override
    public int getLength(PigNullableWritable w) {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + 8 bytes time + 2 bytes timezone + mIndex
        return 12;
      }
    }
  }

  public static class NullableBytesWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  public static class NullableBooleanWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {

    @Override
    public int getLength(PigNullableWritable w) {
      if (w.isNull()) {
        // mNull + mIndex
        return 2;
      } else {
        // mNull + mValue (boolean) + mIndex
        return 3;
      }
    }
  }

  /**
   * Note: NullableBigIntegerWritable is only comparable in native when group only
   */
  public static class NullableBigIntegerWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  /**
   * Note: NullableBigDecimalWritable is only comparable in native when group only
   */  
	public static class NullableBigDecimalWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

}
