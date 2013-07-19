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
	Job job = null;

	public static class ValueMapper<KTYPE, VTYPE> extends
			Mapper<KTYPE, VTYPE, KTYPE, VTYPE> {
		public void map(KTYPE key, VTYPE value, Context context)
				throws IOException, InterruptedException {
			System.err.println(key.getClass().getName()+"\t"+key + "\t" + value);
//			context.write(key, value);
		}
	}

	public void runJob() throws Exception{
		System.out.println(job.getJobName()+"\trunning.......");
		job.waitForCompletion(true);
	}

	public void setJob(Configuration conf, String jobname) {
		try {
			Class<?> keyclass = Class.forName(conf
					.get(KVTest.NATIVETASK_KVTEST_CONF_KEYCLASS));
			Class<?> valueclass = Class.forName(conf
					.get(KVTest.NATIVETASK_KVTEST_CONF_VALUECLASS));
			job = new Job(conf, jobname);
			job.setJarByClass(KVJob.class);
			job.setMapperClass(KVJob.ValueMapper.class);
			job.setOutputKeyClass(keyclass);
			job.setOutputValueClass(valueclass);
			String InputFilePath = conf
					.get(KVTest.NATIVETASK_KVTEST_CONF_INPUTDIR);
			String OutputFilePath = conf
					.get(KVTest.NATIVETASK_KVTEST_CONF_OUTPUTDIR);
			if (conf.get(KVTest.NATIVETASK_KVTEST_CONF_CREATEFILE, "false")
					.equals("true")) {
				FileSystem fs = FileSystem.get(conf);
				fs.delete(new Path(InputFilePath));
				GenTestFile
						.createSequenceTestFile(
								InputFilePath,
								keyclass,
								valueclass,
								Integer.valueOf(conf
										.get(KVTest.NATIVETASK_KVTEST_CONF_FILE_RECORDNUM)));
			}
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.addInputPath(job, new Path(InputFilePath));
			FileOutputFormat.setOutputPath(job, new Path(OutputFilePath));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
