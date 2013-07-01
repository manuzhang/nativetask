package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeMapTaskDelegator;
import org.apache.hadoop.mapred.nativetask.NativeReduceTaskDelegator;

public class KVMappers {
	public static class IntKeyMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			IntWritable testInt = new IntWritable(r.nextInt());
			context.write(testInt, testInt);
		}
	}
	public static class DoubleKeyMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			DoubleWritable testDouble = new DoubleWritable(r.nextDouble());
			context.write(testDouble, testDouble);
		}
	}
	public static class BooleanKeyMapper extends Mapper<Object, Text, BooleanWritable, BooleanWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			BooleanWritable testBoolean = new BooleanWritable(r.nextBoolean());
			context.write(testBoolean, testBoolean);
		}
	}
	public static class FloatKeyMapper extends Mapper<Object, Text, FloatWritable, FloatWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			FloatWritable testFloat = new FloatWritable(r.nextFloat());
			context.write(testFloat, testFloat);
		}
	}
	public static class LongKeyMapper extends Mapper<Object, Text, LongWritable, LongWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			LongWritable testLong = new LongWritable(r.nextLong());
			context.write(testLong, testLong);
		}
	}
	public static class ObjectKeyMapper extends Mapper<Object, Text, ObjectWritable, ObjectWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			ObjectWritable testInt = new ObjectWritable(r.nextGaussian());
			context.write(testInt, testInt);
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub\
		Configuration conf = new Configuration();
		if(args.length!=3){
			System.err.println("Usage: KVtest <keytype> <inputFilePath> <outputFilePath>");
		}else{
			conf.set("native.mapoutput.collector.enabled","true");
			conf.set("native.task.enabled","true");
			conf.set(Constants.MAPRED_REDUCETASK_DELEGATOR_CLASS,
          NativeReduceTaskDelegator.class.getCanonicalName());
			conf.set("native.recordwriter.class","NativeTask.TextIntRecordWriter");
			Job job = new Job(conf, "KVtest "+args[0]);
		    job.setJarByClass(KVMappers.class);
		    job.setMapperClass((Class<? extends Mapper>) Class.forName("org.apache.hadoop.mapred.nativetask.tools.KVtest$"+args[0]+"KeyMapper"));
		    job.setOutputKeyClass(Class.forName("org.apache.hadoop.io."+args[0]+"Writable"));
		    job.setOutputValueClass(Class.forName("org.apache.hadoop.io."+args[0]+"Writable"));
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

}
