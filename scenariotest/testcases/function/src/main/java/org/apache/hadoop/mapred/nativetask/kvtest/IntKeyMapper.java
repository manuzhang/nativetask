package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntKeyMapper {
	public static final String inputFileDir = "/kvint/input/";
	public static final String outputFileDir = "/kvint/output/";

	public static class ValueGen {
		public static <VType> void createHDFSintFile(String genfilepath,
				VType instance, int lineNum) throws IOException{
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			FSDataOutputStream os = hdfs.create(new Path(genfilepath));
			Random r = new Random();
			for (int i = 0; i < lineNum; i++) {
				String linecontent = new IntWritable(i) + "\t" + instance
						+ "\n";
				os.write(linecontent.getBytes("utf-8"));
			}
			os.close();
			hdfs.close();
		}
	}

	public static class ValueMapper<VTYPE> extends
			Mapper<Object, Text, IntWritable, VTYPE> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				String classname = conf.get(KVTest.NATIVETASK_TEST_VALUECLASS);
				Class<?> valueclass = Class.forName(classname);
				String[] input = value.toString().split("\t");
				IntWritable keyout = new IntWritable(Integer.valueOf(input[0]));
				VTYPE valueout = (VTYPE)(valueclass.newInstance());
				context.write(keyout, valueout);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static <VType> Job getIntTestJob(VType instance) {
		Configuration conf = new Configuration();
		conf.addResource(KVTest.NATIVETASK_KVTEST_CONF_PATH);
		conf.set(KVTest.NATIVETASK_TEST_VALUECLASS, instance.getClass().getName());
		System.out.println("*********instance type:" + instance.getClass());
		Job job = null;
		try {
			@SuppressWarnings("rawtypes")
			Class<? extends Mapper> mapClass = new IntKeyMapper.ValueMapper<VType>()
					.getClass();
			job = new Job(conf, "IntKeyTest");
			job.setJarByClass(IntKeyMapper.class);
			job.setMapperClass(mapClass);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(instance.getClass());
			String currentInputFilePath = inputFileDir
					+ instance.getClass().getName();
			ValueGen.createHDFSintFile(currentInputFilePath, instance, 10);
			FileInputFormat.addInputPath(job, new Path(currentInputFilePath));
			FileOutputFormat.setOutputPath(job, new Path(outputFileDir
					+ instance.getClass().getName()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return job;
	}
}
