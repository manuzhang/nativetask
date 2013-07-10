package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BytesKeyMapper {
	public static final String inputFileDir = "/kvBytes/input/";
	public static final String outputFileDir = "/kvBytes/output/";

	public static class ValueMapper<VTYPE> extends
			Mapper<Object, Text, BytesWritable, VTYPE> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String valueclassname = conf.get(KVTest.NATIVETASK_TEST_VALUECLASS);
			try {
				Class<?> valueclass = Class.forName(valueclassname);
				String[] input = value.toString().split("\t");
				BytesWritable keyout = new BytesWritable(
						input[0].getBytes());
				VTYPE valueout = (VTYPE) valueclass.newInstance();
				context.write(keyout, valueout);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static <VType> Job getBytesTestJob(VType instance) {
		Configuration conf = new Configuration();
		conf.addResource(KVTest.NATIVETASK_KVTEST_CONF_PATH);
		conf.set(KVTest.NATIVETASK_TEST_VALUECLASS, instance.getClass()
				.getName());
		System.out.println("*********instance type:" + instance.getClass());
		Job job = null;
		try {
			job = new Job(conf, "BytesKeyTest");
			job.setJarByClass(BytesKeyMapper.class);
			job.setMapperClass(BytesKeyMapper.ValueMapper.class);
			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(instance.getClass());
			String currentInputFilePath = inputFileDir
					+ instance.getClass().getName();
			GenTestFile.createHDFSintFile(currentInputFilePath,
					BytesWritable.class, instance.getClass(), 10);
			FileInputFormat.addInputPath(job, new Path(currentInputFilePath));
			FileOutputFormat.setOutputPath(job, new Path(outputFileDir
					+ instance.getClass().getName()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return job;
	}
}
