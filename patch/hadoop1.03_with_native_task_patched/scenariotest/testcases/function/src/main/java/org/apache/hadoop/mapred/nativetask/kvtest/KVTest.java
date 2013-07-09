package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class KVTest {
	public static final String NATIVETASK_TEST_VALUECLASS = "nativetask.test.valueclass";
	private Class<?>[] valueclasses = {IntWritable.class,LongWritable.class,DoubleWritable.class,FloatWritable.class,
										VIntWritable.class,VLongWritable.class,BooleanWritable.class,Text.class};
	@Test
	public void testIntKey() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(IntKeyMapper.inputFileDir));
		fs.delete(new Path(IntKeyMapper.outputFileDir));
		fs.close();
		for(Class<?> valueclass:valueclasses){
			Job job =  IntKeyMapper.getIntTestJob(valueclass.newInstance());
			job.waitForCompletion(true);
		}
	}
	@Test
	public void testFloatKey() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(FloatKeyMapper.inputFileDir));
		fs.delete(new Path(FloatKeyMapper.outputFileDir));
		fs.close();
		for(Class<?> valueclass:valueclasses){
			Job job =  FloatKeyMapper.getFloatTestJob(valueclass.newInstance());
			job.waitForCompletion(true);
		}
	}
	@Test
	public void testDoubleKey() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(DoubleKeyMapper.inputFileDir));
		fs.delete(new Path(DoubleKeyMapper.outputFileDir));
		fs.close();
		for(Class<?> valueclass:valueclasses){
			Job job =  DoubleKeyMapper.getDoubleTestJob(valueclass.newInstance());
			job.waitForCompletion(true);
		}
	}
	@Before
	public void startUp(){
		
	}
}
