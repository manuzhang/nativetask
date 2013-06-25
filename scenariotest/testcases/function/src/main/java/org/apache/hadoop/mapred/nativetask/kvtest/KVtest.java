package scenario.compatibility;

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

import test.scenario.compatibility.WordCount.IntSumReducer;

public class KVtest {
	public static class IntKeyTest extends Mapper<Object, Text, IntWritable, IntWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			IntWritable testInt = new IntWritable(r.nextInt());
			context.write(testInt, testInt);
		}
	}
	public static class DoubleKeyTest extends Mapper<Object, Text, DoubleWritable, DoubleWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			DoubleWritable testDouble = new DoubleWritable(r.nextDouble());
			context.write(testDouble, testDouble);
		}
	}
	public static class BooleanKeyTest extends Mapper<Object, Text, BooleanWritable, BooleanWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			BooleanWritable testBoolean = new BooleanWritable(r.nextBoolean());
			context.write(testBoolean, testBoolean);
		}
	}
	public static class FloatKeyTest extends Mapper<Object, Text, FloatWritable, FloatWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			FloatWritable testFloat = new FloatWritable(r.nextFloat());
			context.write(testFloat, testFloat);
		}
	}
	public static class LongKeyTest extends Mapper<Object, Text, LongWritable, LongWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			Random r = new Random();
			LongWritable testLong = new LongWritable(r.nextLong());
			context.write(testLong, testLong);
		}
	}
	public static class ObjectKeyTest extends Mapper<Object, Text, ObjectWritable, ObjectWritable>{
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
			Job job = new Job(conf, "KVtest "+args[0]);
		    job.setJarByClass(KVtest.class);
		    job.setMapperClass((Class<? extends Mapper>) Class.forName(args[0]+"KeyTest"));
		    job.setOutputKeyClass(Class.forName(args[0]+"Writable"));
		    job.setOutputValueClass(Class.forName(args[0]+"Writable"));
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

}
