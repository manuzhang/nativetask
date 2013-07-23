package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVJob {
	public static final String FILE_INPUTPATH_CONF_KEY = "nativetask.kvtest.inputfile.path";
	public static final String FILE_OUTPUTPATH_CONF_KEY = "nativetask.kvtest.outputfile.path";
	Job job = null;

	public static class ValueMapper<KTYPE, VTYPE> extends
			Mapper<KTYPE, VTYPE, KTYPE, VTYPE> {
		public void map(KTYPE key, VTYPE value, Context context)
				throws IOException, InterruptedException {
//			System.out.println("map read: "+key+"\t"+value);
			context.write(key, value);
		}
	}

	public KVJob(String jobname, Configuration conf, Class<?> keyclass,
			Class<?> valueclass) throws Exception {
		// TODO Auto-generated constructor stub
		job = new Job(conf, jobname);
		job.setJarByClass(KVJob.class);
		job.setMapperClass(KVJob.ValueMapper.class);
		job.setOutputKeyClass(keyclass);
		job.setOutputValueClass(valueclass);

		String fileinputpath = conf.get(FILE_INPUTPATH_CONF_KEY, "");
		String fileoutputpath = conf.get(FILE_OUTPUTPATH_CONF_KEY, "");

		System.out.println(fileinputpath);
		System.out.println(fileoutputpath);
		if (conf.get(KVTest.NATIVETASK_KVTEST_CONF_CREATEFILE).equals("true")) {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(fileinputpath));
			fs.close();
			System.out.println("file deleted! " + fileinputpath);
			TestFile testfile = new TestFile(Integer.valueOf(conf.get(
					TestFile.FILESIZE_CONF_KEY, "1000")), fileinputpath,
					keyclass.getName(), valueclass.getName());
			testfile.createSequenceTestFile();
			System.out.println("file created! " + fileinputpath);
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(fileinputpath));
		FileOutputFormat.setOutputPath(job, new Path(fileoutputpath));
	}

	public void runJob() throws Exception {
		System.out.println(job.getJobName() + "\trunning.......");
		job.waitForCompletion(true);
	}
	// @Deprecated
	// public void setJob(Configuration conf, String jobname) {
	// try {
	// Class<?> keyclass = Class.forName(conf
	// .get(KVTest.NATIVETASK_KVTEST_CONF_KEYCLASS));
	// Class<?> valueclass = Class.forName(conf
	// .get(KVTest.NATIVETASK_KVTEST_CONF_VALUECLASS));
	// job = new Job(conf, jobname);
	// job.setJarByClass(KVJob.class);
	// job.setMapperClass(KVJob.ValueMapper.class);
	// job.setOutputKeyClass(keyclass);
	// job.setOutputValueClass(valueclass);
	// String InputFilePath = conf
	// .get(KVTest.NATIVETASK_KVTEST_CONF_INPUTDIR);
	// String OutputFilePath = conf
	// .get(KVTest.NATIVETASK_KVTEST_CONF_OUTPUTDIR);
	// if (conf.get(KVTest.NATIVETASK_KVTEST_CONF_CREATEFILE, "false")
	// .equals("true")) {
	// FileSystem fs = FileSystem.get(conf);
	// fs.delete(new Path(InputFilePath));
	// TestFile testfile = new TestFile(1000, OutputFilePath,
	// keyclass.getName(), valueclass.getName());
	// testfile.createSequenceTestFile();
	// }
	// job.setInputFormatClass(SequenceFileInputFormat.class);
	// SequenceFileInputFormat.addInputPath(job, new Path(InputFilePath));
	// FileOutputFormat.setOutputPath(job, new Path(OutputFilePath));
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
}
