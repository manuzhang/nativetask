package org.apache.hadoop.mapred.nativetask.kvtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
import org.apache.hadoop.mapred.nativetask.testframe.util.ResultVerifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KVTest {
	public static final String NATIVETASK_KVTEST_CONF_KEYCLASS = "nativetask.kvtest.keyclass";
	public static final String NATIVETASK_KVTEST_CONF_VALUECLASS = "nativetask.kvtest.valueclass";
	private static Class<?>[] keyclasses = null;
	private static Class<?>[] valueclasses = null;
	private static String[] keyclassNames = null;
	private static String[] valueclassNames = null;
	public static final String NATIVETASK_KVTEST_CONF_PATH = "kvtest-conf.xml";
	public static final String NATIVETASK_KVTEST_CONF_INPUTDIR = "nativetask.kvtest.inputdir";
	public static final String NATIVETASK_KVTEST_CONF_OUTPUTDIR = "nativetask.kvtest.outputdir";
	public static final String NATIVETASK_KVTEST_CONF_NORMAL_OUTPUTDIR = "normal.kvtest.outputdir";
	public static final String NATIVETASK_KVTEST_CONF_CREATEFILE = "nativetask.kvtest.createfile";
	public static final String NATIVETASK_KVTEST_CONF_FILE_RECORDNUM = "nativetask.kvtest.file.recordnum";
	public static final String NATIVETASK_KVTEST_CONF_KEYCLASSES = "nativetask.kvtest.keyclasses";
	public static final String NATIVETASK_KVTEST_CONF_VALUECLASSES = "nativetask.kvtest.valueclasses";
	public static final String NATIVETASK_COLLECTOR_DELEGATOR = "mapreduce.map.output.collector.delegator.class";
	public static final String NATIVETASK_COLLECTOR_DELEGATOR_CLASS = "org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator";

	@Parameters
	public static Iterable<Class<?>[]> data() {
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		String valueclassesStr = conf.get(NATIVETASK_KVTEST_CONF_VALUECLASSES);
		valueclassNames = valueclassesStr.trim().split("\n");
		valueclasses = new Class<?>[valueclassNames.length];
		for (int i = 0; i < valueclassNames.length; i++) {
			try {
				valueclasses[i] = Class.forName(valueclassNames[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		String keyclassesStr = conf.get(NATIVETASK_KVTEST_CONF_KEYCLASSES);
		keyclassNames = keyclassesStr.trim().split("\n");
		keyclasses = new Class<?>[keyclassNames.length];
		for (int i = 0; i < valueclassNames.length; i++) {
			try {
				keyclasses[i] = Class.forName(keyclassNames[i]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(keyclassNames.length + ":" + valueclassNames.length);
		Class<?>[][] kvgroup = new Class<?>[keyclassNames.length
				* valueclassNames.length][2];
		for (int i = 0; i < keyclassNames.length; i++) {
			int tmpindex = i * valueclassNames.length;
			for (int j = 0; j < valueclassNames.length; j++) {
				kvgroup[tmpindex + j][0] = keyclasses[i];
				kvgroup[tmpindex + j][1] = valueclasses[j];
			}
		}
		return Arrays.asList(kvgroup);
	}

	private Class<?> keyclass;
	private Class<?> valueclass;

	public KVTest(Class<?> keyclass, Class<?> valueclass) {
		this.keyclass = keyclass;
		this.valueclass = valueclass;
	}

	@Test
	public void test() {
		System.out.println(keyclass + ":" + valueclass);
		try {
			String nativeoutput = this.runNativeTest(keyclass.getSimpleName()
					+ "--" + valueclass.getSimpleName(), keyclass, valueclass);
			String normaloutput = this.runNormalTest(keyclass.getSimpleName()
					+ "--" + valueclass.getSimpleName(), keyclass, valueclass);
			boolean compareRet = ResultVerifier.verify(normaloutput,
					nativeoutput);
			assertEquals(
					"file compare result: if they are the same ,then return true",
					true, compareRet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// @Test
	// public void testIntKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeIntKeyTest",
	// IntWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalIntKeyTest",
	// IntWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testFloatKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeFloatKeyTest",
	// FloatWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalFloatKeyTest",
	// FloatWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testDoubleKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeDoubleKeyTest",
	// DoubleWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalDoubleKeyTest",
	// DoubleWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testLongKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeLongKeyTest",
	// LongWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalLongKeyTest",
	// LongWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testVIntKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeVIntKeyTest",
	// VIntWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalVIntKeyTest",
	// VIntWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testVLongKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeVLongKeyTest",
	// VLongWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalVLongKeyTest",
	// VLongWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testBooleanKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeBooleanKeyTest",
	// BooleanWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalBooleanKeyTest",
	// BooleanWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testTextKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeTextKeyTest",
	// Text.class);
	// String[] normalOutputfiles = runNormalTest("NormalTextKeyTest",
	// Text.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testByteKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeByteKeyTest",
	// ByteWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalByteKeyTest",
	// ByteWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }
	//
	// @Test
	// public void testBytesKey() throws InstantiationException,
	// IllegalAccessException, ClassNotFoundException, IOException,
	// InterruptedException {
	// String[] nativeOutputfiles = runNativeTest("NativeBytesKeyTest",
	// BytesWritable.class);
	// String[] normalOutputfiles = runNormalTest("NormalBytesKeyTest",
	// BytesWritable.class);
	// compareResult(normalOutputfiles, nativeOutputfiles);
	// }

	@Before
	public void startUp() {

	}

	private String runNativeTest(String jobname, Class<?> keyclass,
			Class<?> valueclass) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		conf.set(NATIVETASK_COLLECTOR_DELEGATOR,
				NATIVETASK_COLLECTOR_DELEGATOR_CLASS);
		String inputpath = conf.get(NATIVETASK_KVTEST_CONF_INPUTDIR) + "/"
				+ keyclass.getName() + "/" + valueclass.getName();
		String outputpath = conf.get(NATIVETASK_KVTEST_CONF_OUTPUTDIR) + "/"
				+ keyclass.getName() + "/" + valueclass.getName();
		conf.set(KVJob.FILE_INPUTPATH_CONF_KEY, inputpath);
		conf.set(KVJob.FILE_OUTPUTPATH_CONF_KEY, outputpath);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputpath));
		fs.close();
		conf.set(NATIVETASK_KVTEST_CONF_CREATEFILE, "true");
		try {
			KVJob keyJob = new KVJob(jobname, conf, keyclass, valueclass);
			keyJob.runJob();
		} catch (Exception e) {
			return "native testcase run time error.";
		}
		return outputpath;
	}

	private String runNormalTest(String jobname, Class<?> keyclass,
			Class<?> valueclass) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(NATIVETASK_KVTEST_CONF_PATH);
		String inputpath = conf.get(NATIVETASK_KVTEST_CONF_INPUTDIR) + "/"
				+ keyclass.getName() + "/" + valueclass.getName();
		String outputpath = conf.get(NATIVETASK_KVTEST_CONF_NORMAL_OUTPUTDIR)
				+ "/" + keyclass.getName() + "/" + valueclass.getName();
		conf.set(KVJob.FILE_INPUTPATH_CONF_KEY, inputpath);
		conf.set(KVJob.FILE_OUTPUTPATH_CONF_KEY, outputpath);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputpath));
		fs.close();
		conf.set(NATIVETASK_KVTEST_CONF_CREATEFILE, "false");
		try {
			KVJob keyJob = new KVJob(jobname, conf, keyclass, valueclass);
			keyJob.runJob();
		} catch (Exception e) {
			return "native testcase run time error.";
		}
		return outputpath;
	}

	// private void compareResult(String[] normalfiles, String[] nativefiles) {
	// if (normalfiles.length != nativefiles.length) {
	// assertEquals("normal file num equals native file num",
	// "normal file num do not equals native file num");
	// }
	// String expected = "";
	// String factual = "";
	// System.err.println("run result - file num:" + nativefiles.length + " "
	// + normalfiles.length);
	// for (int i = 0; i < nativefiles.length; i++) {
	// System.err.println("run result - file name:" + nativefiles[i] + " "
	// + normalfiles[i]);
	// expected += "1,";
	// factual += ResultVerifier.verify(nativefiles[i], normalfiles[i])
	// + ",";
	// }
	// System.err.println("run result:" + expected + " " + factual);
	// assertEquals(expected, factual);
	// }
	private void compareResult(String normalfile, String nativefile) {
		try {
			boolean compareRet = ResultVerifier.verify(normalfile, nativefile);
			// assertEquals(
			// "file compare result: if they are the same ,then return true",
			// true, compareRet);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
