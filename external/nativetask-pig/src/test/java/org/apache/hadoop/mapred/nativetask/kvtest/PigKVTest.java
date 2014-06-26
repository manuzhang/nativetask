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
package org.apache.hadoop.mapred.nativetask.kvtest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigBooleanWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigCharArrayWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigDBAWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigDateTimeWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigDoubleWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigFloatWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigIntWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigLongWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigTupleWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBooleanRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBytesRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDateTimeRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDoubleRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFloatRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigIntRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigLongRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleSortComparator;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDateTimeWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PigKVTest {


  private static Configuration nativekvtestconf = null;
  private static Configuration hadoopkvtestconf = null;

  private static Map<Class, Class> classToRawComparator = new HashMap<Class, Class>();
  private static Map<Class, Class> classToComparator = new HashMap<Class, Class>();

  private static final String PIG_KEY_ORDER = "pig.key.order";

  static {
    nativekvtestconf = ScenarioConfiguration.getNativeConfiguration();
    nativekvtestconf.addResource(TestConstants.PIG_KVTEST_CONF_PATH);
    ;
    hadoopkvtestconf = ScenarioConfiguration.getNormalConfiguration();
    hadoopkvtestconf.addResource(TestConstants.PIG_KVTEST_CONF_PATH);
    ;

    classToRawComparator.put(NullableBooleanWritable.class,
      PigBooleanRawComparator.class);
    classToRawComparator.put(NullableBytesWritable.class,
      PigBytesRawComparator.class);
    classToRawComparator.put(NullableDateTimeWritable.class,
      PigDateTimeRawComparator.class);
    classToRawComparator.put(NullableDoubleWritable.class,
      PigDoubleRawComparator.class);
    classToRawComparator.put(NullableFloatWritable.class,
      PigFloatRawComparator.class);
    classToRawComparator.put(NullableIntWritable.class,
      PigIntRawComparator.class);
    classToRawComparator.put(NullableLongWritable.class,
      PigLongRawComparator.class);
    classToRawComparator.put(NullableText.class,
      PigTextRawComparator.class);
    classToRawComparator.put(NullableTuple.class,
      PigTupleSortComparator.class);

    classToComparator.put(NullableBooleanWritable.class,
      PigBooleanWritableComparator.class);
    classToComparator.put(NullableBytesWritable.class,
      PigDBAWritableComparator.class);
    classToComparator.put(NullableDateTimeWritable.class,
      PigDateTimeWritableComparator.class);
    classToComparator.put(NullableDoubleWritable.class,
      PigDoubleWritableComparator.class);
    classToComparator.put(NullableFloatWritable.class,
      PigFloatWritableComparator.class);
    classToComparator.put(NullableIntWritable.class,
      PigIntWritableComparator.class);
    classToComparator.put(NullableLongWritable.class,
      PigLongWritableComparator.class);
    classToComparator.put(NullableText.class,
      PigCharArrayWritableComparator.class);
    classToComparator.put(NullableTuple.class,
      PigTupleWritableComparator.class);
  }

  @Parameters(name = "key:{0}\nvalue:{1}\norder:{2}")
  public static Iterable<Object[]> data() {
    Class<?>[] keyclasses = null;
    Class<?>[] valueclasses = null;
    String[] keyclassNames = null;
    String[] valueclassNames = null;
    String[] keyOrderNames = null;

    final String valueclassesStr = nativekvtestconf
      .get(TestConstants.NATIVETASK_KVTEST_VALUECLASSES);
    valueclassNames = valueclassesStr.replaceAll("\\s", "").split(";");// delete
    // " "
    final ArrayList<Class<?>> tmpvalueclasses = new ArrayList<Class<?>>();
    for (int i = 0; i < valueclassNames.length; i++) {
      try {
        if (valueclassNames[i].equals("")) {
          continue;
        }
        tmpvalueclasses.add(Class.forName(valueclassNames[i]));
      } catch (final ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    valueclasses = tmpvalueclasses.toArray(new Class[tmpvalueclasses.size()]);
    final String keyclassesStr = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_KEYCLASSES);
    keyclassNames = keyclassesStr.replaceAll("\\s", "").split(";");// delete
    // " "
    final ArrayList<Class<?>> tmpkeyclasses = new ArrayList<Class<?>>();
    for (int i = 0; i < keyclassNames.length; i++) {
      try {
        if (keyclassNames[i].equals("")) {
          continue;
        }
        tmpkeyclasses.add(Class.forName(keyclassNames[i]));
      } catch (final ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    keyclasses = tmpkeyclasses.toArray(new Class[tmpkeyclasses.size()]);

    final String keyOrderStr = nativekvtestconf.get(PIG_KEY_ORDER);
    keyOrderNames = keyOrderStr.replaceAll("\\s", "").split(";");

    final int kclen = keyclassNames.length;
    final int vclen = valueclassNames.length;
    final int kolen = keyOrderNames.length;

    final Object[][] kvgroup = new Object[kclen * vclen * kolen][3];
    for (int i = 0; i < keyclassNames.length; i++) {
      for (int j = 0; j < valueclassNames.length; j++) {
        for (int k = 0; k < keyOrderNames.length; k++) {
          final int index = i * vclen * kolen + (j * kolen + k);
          kvgroup[index][0] = keyclasses[i];
          kvgroup[index][1] = valueclasses[j];
          kvgroup[index][2] = keyOrderNames[k];
        }
      }
    }
    return Arrays.asList(kvgroup);
  }

  private final Class<?> keyclass;
  private final Class<?> valueclass;
  private final String keyOrder;

  public PigKVTest(Object keyclass, Object valueclass, Object keyOrder) {
    this.keyclass = (Class<?>)keyclass;
    this.valueclass = (Class<?>)valueclass;
    this.keyOrder = (String)keyOrder;

  }

  @Test
  public void testKVCompatibility() {

    try {
      // the same as sortOrderAsc for keyclasses other than NullableTuple
      if (keyOrder.equals("sortOrderAscDesc") && keyclass != NullableTuple.class) {
        return;
      }
      final String nativeoutput = this.runNativeTest(
        "Test:" + keyclass.getSimpleName() + "--" + valueclass.getSimpleName(), keyclass, valueclass);
      final String normaloutput = this.runNormalTest(
        "Test:" + keyclass.getSimpleName() + "--" + valueclass.getSimpleName(), keyclass, valueclass);
      final boolean compareRet = ResultVerifier.verify(normaloutput, nativeoutput);
      final String input = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
        + keyclass.getName()
        + "/" + valueclass.getName();
      if(compareRet){
        final FileSystem fs = FileSystem.get(hadoopkvtestconf);
        fs.delete(new Path(nativeoutput), true);
        fs.delete(new Path(normaloutput), true);
        fs.delete(new Path(input), true);
        fs.close();
      }
      assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
    } catch (final IOException e) {
      assertEquals("test run exception:", null, e);
    } catch (final Exception e) {
      assertEquals("test run exception:", null, e);
    }
  }

  @Before
  public void startUp() {

  }

  private String runNativeTest(String jobname, Class<?> keyclass, Class<?> valueclass) throws IOException {
    final String inputpath = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
      + keyclass.getName()
      + "/" + valueclass.getName();
    final String outputpath = nativekvtestconf.get(TestConstants.NATIVETASK_KVTEST_OUTPUTDIR) + "/"
      + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(nativekvtestconf);
    fs.delete(new Path(outputpath));
    fs.close();
    nativekvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "true");
    if (keyOrder.equals("groupOnly")) {
      nativekvtestconf.setBoolean(Constants.PIG_GROUP_ONLY, true);
    } else {
      nativekvtestconf.setBoolean(Constants.PIG_GROUP_ONLY, false);
    }
    try {
      final KVJob keyJob = new KVJob(jobname, nativekvtestconf, keyclass, valueclass, inputpath, outputpath);
      setPigJobConf(keyJob.job);
      keyJob.runJob();
    } catch (final Exception e) {
      return "native testcase run time error.";
    }
    return outputpath;
  }

  private String runNormalTest(String jobname, Class<?> keyclass, Class<?> valueclass) throws IOException {
    final String inputpath = hadoopkvtestconf.get(TestConstants.NATIVETASK_KVTEST_INPUTDIR) + "/"
      + keyclass.getName()
      + "/" + valueclass.getName();
    final String outputpath = hadoopkvtestconf
      .get(TestConstants.NATIVETASK_KVTEST_NORMAL_OUTPUTDIR)
      + "/"
      + keyclass.getName() + "/" + valueclass.getName();
    // if output file exists ,then delete it
    final FileSystem fs = FileSystem.get(hadoopkvtestconf);
    fs.delete(new Path(outputpath));
    fs.close();
    hadoopkvtestconf.set(TestConstants.NATIVETASK_KVTEST_CREATEFILE, "false");
    try {
      final KVJob keyJob = new KVJob(jobname, hadoopkvtestconf, keyclass, valueclass, inputpath, outputpath);
      setPigJobConf(keyJob.job);
      keyJob.runJob();
    } catch (final Exception e) {
      return "normal testcase run time error.";
    }
    return outputpath;
  }


  private void setPigConf(Configuration conf) {
    if (keyOrder.equals("sortOrderAsc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER, ObjectSerializer.serialize(new boolean[] { true }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (keyOrder.equals("sortOrderDesc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER, ObjectSerializer.serialize(new boolean[] { false }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (keyOrder.equals("sortOrderAscDesc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER,
          ObjectSerializer.serialize(new boolean[] { true, false }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void setPigJobConf(Job job) {
    Configuration conf = job.getConfiguration();

    if (keyOrder.equals("groupOnly")) {
      job.setSortComparatorClass(classToComparator.get(keyclass));
    } else {
      job.setSortComparatorClass(classToRawComparator.get(keyclass));
    }

    if (keyOrder.equals("sortOrderAsc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER, ObjectSerializer.serialize(new boolean[] { true }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (keyOrder.equals("sortOrderDesc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER, ObjectSerializer.serialize(new boolean[] { false }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (keyOrder.equals("sortOrderAscDesc")) {
      try {
        conf.set(Constants.PIG_SORT_ORDER,
          ObjectSerializer.serialize(new boolean[] { true, false }));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
