package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextKeyMapper {
	public static final String inputFileDir = "/kvText/input/";
	public static final String outputFileDir = "/kvText/output/";

	public static class ValueMapper<VTYPE> extends
			Mapper<Object, Text, Text, VTYPE> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String valueclassname = conf.get(KVTest.NATIVETASK_TEST_VALUECLASS);
			try {
				Class<?> valueclass = Class.forName(valueclassname);
				String[] input = value.toString().split("\t");
				Text keyout = new Text(
						input[0]);
				VTYPE valueout = (VTYPE) valueclass.newInstance();
				context.write(keyout, valueout);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static <VType> Job getTextTestJob(VType instance) {
		Configuration conf = new Configuration();
		conf.addResource(KVTest.NATIVETASK_KVTEST_CONF_PATH);
		conf.set(KVTest.NATIVETASK_TEST_VALUECLASS, instance.getClass()
				.getName());
		System.out.println("*********instance type:" + instance.getClass());
		Job job = null;
		try {
			job = new Job(conf, "TextKeyTest");
			job.setJarByClass(TextKeyMapper.class);
			job.setMapperClass(TextKeyMapper.ValueMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(instance.getClass());
			String currentInputFilePath = inputFileDir
					+ instance.getClass().getName();
			GenTestFile.createHDFSintFile(currentInputFilePath,
					Text.class, instance.getClass(), 10);
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
