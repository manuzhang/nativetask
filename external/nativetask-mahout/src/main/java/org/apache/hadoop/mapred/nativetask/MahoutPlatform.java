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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.DefaultSerializer;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.LongWritableSerializer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;

public class MahoutPlatform extends Platform {

  private static final Logger LOG = Logger.getLogger(MahoutPlatform.class);
  private Map<String, String> keyClassToComparator = new HashMap<String, String>();
	public static final String DEFAULT_NATIVE_LIBRARY = "MahoutPlatform=libnativetaskmahout.so";

  public MahoutPlatform() {
  }

  @Override
  public void init() throws IOException {
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


    keyClassToComparator.put("org.apache.mahout.common.StringTuple",
      "StringTupleComparator");
    keyClassToComparator.put("org.apache.mahout.vectorizer.collocations.llr.Gram",
      "GramComparator");
    keyClassToComparator.put("org.apache.mahout.vectorizer.collocations.llr.GramKey",
      "GramKeyComparator");
    keyClassToComparator.put("org.apache.mahout.math.hadoop.stochasticsvd.SplitPartitionedWritable",
      "SplitPartitionedComparator");
    keyClassToComparator.put("org.apache.mahout.cf.taste.hadoop.EntityEntityWritable",
      "EntityEntityComparator");
    keyClassToComparator.put("org.apache.mahout.math.VarIntWritable",
      "VarIntComparator");
    keyClassToComparator.put("org.apache.mahout.math.VarLongWritable",
      "VarLongComparator");

    LOG.info("Mahout platform inited");
  }

  @Override
  public String name() {
    return "Mahout";
  }

  @Override
  public boolean support(String keyClassName, INativeSerializer serializer, JobConf job) {
    if (super.support(keyClassName, serializer, job)) {
      Class comparatorClass = job.getClass(MRJobConfig.KEY_COMPARATOR, null, RawComparator.class);
      if (comparatorClass != null) {
        String message = "Native output collector don't support customized java comparator "
          + comparatorClass.getName();
        LOG.error(message);
      } else {
				String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClassName;
				if (keyClassName.equals("org.apache.mahout.classifier.df.mapreduce.partial.TreeID")) {
					job.set(nativeComparator, "MahoutPlatform.NativeObjectFactory::LongComparator");
				} else {
					job.set(nativeComparator, "MahoutPlatform.MahoutPlatform::" + keyClassToComparator.get(keyClassName));
				}
				if (job.get(Constants.NATIVE_CLASS_LIBRARY_BUILDIN) == null) {
					job.set(Constants.NATIVE_CLASS_LIBRARY_BUILDIN, DEFAULT_NATIVE_LIBRARY);
				}
				return true;
			} 
		}
    return false;
  }

  public static class EntityEntityWritableSerializer extends DefaultSerializer
    implements INativeComparable {
  }

  public static class GramKeySerializer extends DefaultSerializer
    implements INativeComparable {
  }

  public static class GramSerializer  extends DefaultSerializer
    implements INativeComparable {
  }

  public static class SplitPartitionedWritableSerializer extends DefaultSerializer
    implements INativeComparable {
  }

  public static class StringTupleSerializer extends DefaultSerializer implements
    INativeComparable {
  }

  public static class VarIntWritableSerializer extends DefaultSerializer implements
    INativeComparable {
  }

  public static class VarLongWritableSerializer extends DefaultSerializer implements
    INativeComparable {
  }
}
