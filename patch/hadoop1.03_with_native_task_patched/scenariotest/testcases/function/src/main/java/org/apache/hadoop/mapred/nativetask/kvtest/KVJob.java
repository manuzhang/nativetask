package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVJob {
	Job job = null;

	public static class ValueMapper<KTYPE,VTYPE> extends
			Mapper<KTYPE, VTYPE, KTYPE, VTYPE> {
		public void map(KTYPE key, VTYPE value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public void runJob() {
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
					.get(KVTest.NATIVETASK_KVTEST_CONF_INPUTDIR)
					+ "/"
					+ conf.get(KVTest.NATIVETASK_KVTEST_CONF_VALUECLASS);
			String OutputFilePath = conf
					.get(KVTest.NATIVETASK_KVTEST_CONF_OUTPUTDIR)
					+ "/"
					+ conf.get(KVTest.NATIVETASK_KVTEST_CONF_VALUECLASS);

			GenTestFile.createSequenceTestFile(InputFilePath, keyclass, valueclass, 10);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.addInputPath(job, new Path(InputFilePath));
			FileOutputFormat.setOutputPath(job, new Path(OutputFilePath));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
