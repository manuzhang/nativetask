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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigPlatform extends Platform {

  private static Log LOG = LogFactory.getLog(PigPlatform.class);

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

  @Override
  public boolean support(INativeSerializer serializer, JobConf job) {
    if (job.getBoolean(Constants.PIG_GROUP_ONLY, false)) {
      LOG.info("Pig key types: group only");
    } else if ((serializer instanceof INativeComparable)
      // don't support user defined comparator
      && !job.getBoolean(Constants.PIG_USER_COMPARATOR, false)) {
      try {
        if (job.get(Constants.PIG_SORT_ORDER, null) != null) {
          boolean[] order = (boolean[]) ObjectSerializer.deserialize(job.get(Constants.PIG_SORT_ORDER));
          job.set(Constants.NATIVE_PIG_SORT, ConfigUtil.booleansToString(order));
          LOG.info("Pig key types: set sort order");
        }
        if (job.get(Constants.PIG_SEC_SORT_ORDER, null) != null) {
          boolean[] order = (boolean[]) ObjectSerializer.deserialize(job
            .get(Constants.PIG_SEC_SORT_ORDER));
          job.set(Constants.NATIVE_PIG_SEC_SORT, ConfigUtil.booleansToString(order));
          job.setBoolean(Constants.NATIVE_PIG_USE_SEC_KEY, true);
          LOG.info("Pig key types: set secondary sort order");
        }
      } catch (final IOException e) {
        LOG.error("cannot deserialize Pig configurations", e);
        return false;
      }
    } else {
      return false;
    }
    return true;
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
