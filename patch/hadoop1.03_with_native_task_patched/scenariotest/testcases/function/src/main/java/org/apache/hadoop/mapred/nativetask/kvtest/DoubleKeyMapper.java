package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.kvtest.FloatKeyMapper.ValueGen;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DoubleKeyMapper {
	public static final String inputFileDir = "/kvdouble/input/";
	public static final String outputFileDir = "/kvdouble/output/";
	public static class ValueMapper<VTYPE> extends Mapper<Object,Text,DoubleWritable,VTYPE>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String valueclassname = conf.get(KVTest.NATIVETASK_TEST_VALUECLASS);
			try {
				Class<?> valueclass = Class.forName(valueclassname);
				String[] input = value.toString().split("\t");
				DoubleWritable keyout = new DoubleWritable(Double.valueOf(input[0]));
				VTYPE valueout = (VTYPE)valueclass.newInstance();
				context.write(keyout,valueout);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static <VType> Job getFloatTestJob(VType instance) {
		Configuration conf = new Configuration();
		conf.addResource("test-conf.xml");
		conf.set(KVTest.NATIVETASK_TEST_VALUECLASS, instance.getClass().getName());
		System.out.println("*********instance type:" + instance.getClass());
		Job job = null;
		try {
			job = new Job(conf, "DoubleKeyTest");
			job.setJarByClass(DoubleKeyMapper.class);
			job.setMapperClass(DoubleKeyMapper.ValueMapper.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(instance.getClass());
			String currentInputFilePath = inputFileDir
					+ instance.getClass().getName();
			GenTestFile.createHDFSintFile(currentInputFilePath, DoubleWritable.class,instance.getClass(), 10);
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
