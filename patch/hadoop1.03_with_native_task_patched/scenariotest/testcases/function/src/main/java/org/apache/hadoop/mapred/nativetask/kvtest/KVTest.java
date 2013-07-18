package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Before;
import org.junit.Test;

public class KVTest extends TestCase {
	public static final String NATIVETASK_KVTEST_CONF_KEYCLASS = "nativetask.kvtest.keyclass";
	public static final String NATIVETASK_KVTEST_CONF_VALUECLASS = "nativetask.kvtest.valueclass";
	private static Class<?>[] valueclasses = null;
	public static final String NATIVETASK_KVTEST_CONF_PATH = "test-kv-conf.xml";
	public static final String NATIVETASK_KVTEST_CONF_INPUTDIR = "nativetask.kvtest.inputdir";
	public static final String NATIVETASK_KVTEST_CONF_OUTPUTDIR = "nativetask.kvtest.outputdir";
	public static final String NATIVETASK_KVTEST_CONF_NORMAL_OUTPUTDIR = "normal.kvtest.outputdir";
	public static final String NATIVETASK_KVTEST_CONF_CREATEFILE = "nativetask.kvtest.createfile";
	public static final String NATIVETASK_KVTEST_CONF_FILE_RECORDNUM = "nativetask.kvtest.file.recordnum";
	public static final String NATIVETASK_KVTEST_CONF_VALUECLASSES = "nativetask.kvtest.valueclasses";
	public static final String NATIVETASK_COLLECTOR_DELEGATOR = "mapreduce.map.output.collector.delegator.class";
	public static final String NATIVETASK_COLLECTOR_DELEGATOR_CLASS = "org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator";

	static {
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		String valueclassesStr = conf.get(NATIVETASK_KVTEST_CONF_VALUECLASSES);
		String[] valueclassNames = valueclassesStr.trim().split("\n");
		valueclasses = new Class<?>[valueclassNames.length];
		for (int i = 0; i < valueclassNames.length; i++) {
			try {
				System.err.println(valueclassNames[i]);
				valueclasses[i] = Class.forName(valueclassNames[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testIntKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeIntKeyTest",
				IntWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalIntKeyTest",
				IntWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testFloatKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeFloatKeyTest",
				FloatWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalFloatKeyTest",
				FloatWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testDoubleKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeDoubleKeyTest",
				DoubleWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalDoubleKeyTest",
				DoubleWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testLongKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeLongKeyTest",
				LongWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalLongKeyTest",
				LongWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testVIntKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeVIntKeyTest",
				VIntWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalVIntKeyTest",
				VIntWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testVLongKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeVLongKeyTest",
				VLongWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalVLongKeyTest",
				VLongWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testBooleanKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeBooleanKeyTest",
				BooleanWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalBooleanKeyTest",
				BooleanWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testTextKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeTextKeyTest",
				Text.class);
		String[] normalOutputfiles = runNormalTest("NormalTextKeyTest",
				Text.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testByteKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeByteKeyTest",
				ByteWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalByteKeyTest",
				ByteWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Test
	public void testBytesKey() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			InterruptedException {
		String[] nativeOutputfiles = runNativeTest("NativeBytesKeyTest",
				BytesWritable.class);
		String[] normalOutputfiles = runNormalTest("NormalBytesKeyTest",
				BytesWritable.class);
		compareResult(normalOutputfiles, nativeOutputfiles);
	}

	@Before
	public void startUp() {

	}

	private String[] runNativeTest(String jobname, Class<?> keyclass)
			throws IOException {
		String[] ret = new String[valueclasses.length];
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		conf.set(NATIVETASK_COLLECTOR_DELEGATOR,
				NATIVETASK_COLLECTOR_DELEGATOR_CLASS);
		String inputdir = conf.get(NATIVETASK_KVTEST_CONF_INPUTDIR) + "/"
				+ keyclass.getName();
		String outputdir = conf.get(NATIVETASK_KVTEST_CONF_OUTPUTDIR) + "/"
				+ jobname;
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputdir));
		fs.close();
		conf.set(NATIVETASK_KVTEST_CONF_CREATEFILE, "true");
		conf.set(NATIVETASK_KVTEST_CONF_KEYCLASS, keyclass.getName());
		for (int i = 0; i < valueclasses.length; i++) {
			conf.set(NATIVETASK_KVTEST_CONF_INPUTDIR, inputdir + "/"
					+ valueclasses[i].getName());
			conf.set(NATIVETASK_KVTEST_CONF_OUTPUTDIR, outputdir + "/"
					+ valueclasses[i].getName());
			conf.set(NATIVETASK_KVTEST_CONF_VALUECLASS,
					valueclasses[i].getName());
			KVJob keyJob = new KVJob();
			keyJob.setJob(conf, jobname);
			try {
				keyJob.runJob();
				ret[i] = outputdir + "/" + valueclasses[i].getName()
						+ "/part-r-00000";
			} catch (Exception e) {
				ret[i] = "native testcase run time error.";
			}
		}
		return ret;
	}

	private String[] runNormalTest(String jobname, Class<?> keyclass)
			throws IOException {
		String[] ret = new String[valueclasses.length];
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		String inputdir = conf.get(NATIVETASK_KVTEST_CONF_INPUTDIR) + "/"
				+ keyclass.getName();
		String outputdir = conf.get(NATIVETASK_KVTEST_CONF_NORMAL_OUTPUTDIR)
				+ "/" + jobname;
		FileSystem fs = FileSystem.get(conf);
		// fs.delete(new Path(inputdir));
		fs.delete(new Path(outputdir));
		fs.close();
		conf.set(NATIVETASK_KVTEST_CONF_KEYCLASS, keyclass.getName());
		for (int i = 0; i < valueclasses.length; i++) {
			conf.set(NATIVETASK_KVTEST_CONF_INPUTDIR, inputdir + "/"
					+ valueclasses[i].getName());
			conf.set(NATIVETASK_KVTEST_CONF_OUTPUTDIR, outputdir + "/"
					+ valueclasses[i].getName());
			conf.set(NATIVETASK_KVTEST_CONF_VALUECLASS,
					valueclasses[i].getName());
			KVJob keyJob = new KVJob();
			keyJob.setJob(conf, jobname);
			try {
				keyJob.runJob();
				ret[i] = outputdir + "/" + valueclasses[i].getName()
						+ "/part-r-00000";
			} catch (Exception e) {
				ret[i] = "normal testcase run time error.";
			}
		}
		return ret;
	}

	private void compareResult(String[] normalfiles, String[] nativefiles) {
		if (normalfiles.length != nativefiles.length) {
			assertEquals("normal file num equals native file num",
					"normal file num do not equals native file num");
		}
		String expected = "";
		String factual = "";
		System.err.println("run result - file num:" + nativefiles.length + " "
				+ normalfiles.length);
		for (int i = 0; i < nativefiles.length; i++) {
			System.err.println("run result - file name:" + nativefiles[i] + " "
					+ normalfiles[i]);
			expected += "1,";
			factual += ResultVerifier.verify(nativefiles[i], normalfiles[i])
					+ ",";
		}
		System.err.println("run result:" + expected + " " + factual);
		assertEquals(expected, factual);
	}
}
