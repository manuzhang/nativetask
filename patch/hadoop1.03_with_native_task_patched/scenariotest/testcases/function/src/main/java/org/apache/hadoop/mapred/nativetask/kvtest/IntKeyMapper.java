package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.NATIVECONF;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntKeyMapper {
	public static class IntValueMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testInt);
		}
	}
	public static class FloatValueMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			FloatWritable testValue = new FloatWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testValue);
		}
	}
	public static class DoubleValueMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			DoubleWritable testValue = new DoubleWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testValue);
		}
	}
	public static class LongValueMapper extends Mapper<Object, Text, IntWritable, LongWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			LongWritable testValue = new LongWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testValue);
		}
	}
	public static class VIntValueMapper extends Mapper<Object, Text, IntWritable, VIntWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			VIntWritable testValue = new VIntWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testValue);
		}
	}
	public static class VLongValueMapper extends Mapper<Object, Text, IntWritable, VLongWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			VLongWritable testValue = new VLongWritable(Integer.valueOf(value.toString()));
			context.write(testInt, testValue);
		}
	}
	public static class BooleanValueMapper extends Mapper<Object, Text, IntWritable, BooleanWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			BooleanWritable testValue = new BooleanWritable(Integer.valueOf(value.toString())%2==0?true:false);
			context.write(testInt, testValue);
		}
	}
	public static class BytesValueMapper extends Mapper<Object, Text, IntWritable, BytesWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			BytesWritable testValue = new BytesWritable(value.toString().getBytes());
			context.write(testInt, testValue);
		}
	}
	public static class ByteValueMapper extends Mapper<Object, Text, IntWritable, ByteWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			ByteWritable testValue = new ByteWritable(value.toString().getBytes()[0]);
			context.write(testInt, testValue);
		}
	}
	public static class NullValueMapper extends Mapper<Object, Text, IntWritable, NullWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			IntWritable testInt = new IntWritable(Integer.valueOf(value.toString()));
			context.write(testInt, null);
		}
	}
	public static Job getIntKeyTestJob(Class<?> valueClass,String inputFilePath){
		Configuration conf = NATIVECONF.getNativeConf();
	    Job job = null;
		try {
			job = new Job(conf, "IntKeytest"+valueClass.getName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("IntKey test job create failed, value type:"+valueClass.getName());
		}
	    job.setJarByClass(KVMappers.class);
	    job.setMapperClass(KVMappers.IntKeyMapper.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(valueClass);
	    try {
			FileInputFormat.addInputPath(job, new Path(inputFilePath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    String currentFilePath = KVTest.outputDir+"intoutput/"+valueClass.getName();
	    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
	    return job;
	}
}

