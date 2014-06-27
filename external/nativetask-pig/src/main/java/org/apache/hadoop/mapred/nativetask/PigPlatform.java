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
import java.util.Map;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigPlatform extends Platform {

  private static Log LOG = LogFactory.getLog(PigPlatform.class);
  public static final String PIG_GROUP_ONLY = "native.pig.groupOnly";
  public static final String PIG_USER_COMPARATOR = "pig.usercomparator";
  public static final String PIG_SORT_ORDER = "pig.sortOrder";
  public static final String NATIVE_PIG_SORT = "native.pig.sortOrder";
  public static final String PIG_SEC_SORT_ORDER = "pig.secondarySortOrder";
  public static final String NATIVE_PIG_SEC_SORT = "native.pig.secondarySortOrder";
  public static final String NATIVE_PIG_USE_SEC_KEY = "native.pig.useSecondaryKey";
  
  private Map<String, String> keyClassToComparator = new HashMap<String, String>();

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

    keyClassToComparator.put("org.apache.pig.impl.io.NullableBooleanWritable",
        "PigNullableBooleanComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableBytesWritable",
        "PigNullableBytesComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableDateTimeWritable",
        "PigNullableDateTimeComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableDoubleWritable",
        "PigNullableDoubleComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableFloatWritable",
        "PigNullableFloatComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableIntWritable", 
        "PigNullableIntComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableLongWritable",
        "PigNullableLongComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableText",
        "PigNullableTextComparator");
    keyClassToComparator.put("org.apache.pig.impl.io.NullableTuple",
        "PigNullableTupleComparator");
  }

  @Override
  public String name() {
    return "Pig";
  }

  @Override
  public boolean support(INativeSerializer serializer, JobConf job) {
    boolean supported = false;
    String keyClass = job.getMapOutputKeyClass().getName();
    String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClass;
    if (job.getBoolean(PIG_GROUP_ONLY, false)) {
      job.set(nativeComparator, "NativeObjectFactory.BytesComparator");
      LOG.info("Pig key types: group only");
      supported = true;
    } else if ((serializer instanceof INativeComparable)
      // don't support user defined comparator
      && !job.getBoolean(PIG_USER_COMPARATOR, false)) {
      try {
        if (job.get(PIG_SORT_ORDER, null) != null) {
          boolean[] order = (boolean[]) ObjectSerializer.deserialize(job.get(PIG_SORT_ORDER));
          job.set(NATIVE_PIG_SORT, ConfigUtil.booleansToString(order));
          job.set(nativeComparator, "PigPlatform." + keyClassToComparator.get(keyClass));
          LOG.info("Pig key types: set sort order");
        }
        if (job.get(PIG_SEC_SORT_ORDER, null) != null) {
          boolean[] order = (boolean[]) ObjectSerializer.deserialize(job
            .get(PIG_SEC_SORT_ORDER));
          job.set(NATIVE_PIG_SEC_SORT, ConfigUtil.booleansToString(order));
          job.setBoolean(NATIVE_PIG_USE_SEC_KEY, true);
          job.set(nativeComparator, "PigPlatform.PigSecondaryKeyComparator");
          LOG.info("Pig key types: set secondary sort order");
        }
        supported = true;
      } catch (final IOException e) {
        LOG.error("cannot deserialize Pig configurations", e);
      }
    } 
    if (supported) {
      job.set(Constants.NATIVE_CLASS_LIBRARY_BUILDIN, "Pig=libnativetask-pig.so");
    }
    return supported;
  }

  private static class NullableWritableSerializer implements
    INativeSerializer<PigNullableWritable> {

    private ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private DataOutputStream outData = new DataOutputStream(outBuffer);
    private int bufferedLength = -1;

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

        // mNull + bufferedLength + mIndex
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

  private static class NullableTupleSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  private static class NullableTextSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  private static class NullableLongWritableSerializer extends NullableWritableSerializer
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

  private static class NullableIntWritableSerializer extends NullableWritableSerializer
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

  private static class NullableFloatWritableSerializer extends NullableWritableSerializer
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

  private static class NullableDoubleWritableSerializer extends NullableWritableSerializer
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

  private static class NullableDateTimeWritableSerializer extends NullableWritableSerializer
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

  private static class NullableBytesWritableSerializer extends NullableWritableSerializer
    implements INativeComparable {
  }

  private static class NullableBooleanWritableSerializer extends NullableWritableSerializer
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

  private static class NullableBigIntegerWritableSerializer extends
    NullableWritableSerializer {
    // not supported as key
  }

  private static class NullableBigDecimalWritableSerializer extends
    NullableWritableSerializer {
    // not supported as key
  }

  private static class NullableBagSerializer extends
    NullableWritableSerializer {
    // not used as key
  }
}
