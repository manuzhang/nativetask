package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.File;

import junit.framework.TestCase;

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
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.NATIVECONF;
import org.apache.hadoop.mapred.nativetask.kvtest.IntKeyMapper;
import org.apache.hadoop.mapred.nativetask.kvtest.KVMappers;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KVTest extends TestCase {
	private static String inputFilePath = "testfile/kvinput.txt";
	public static String outputDir = "testfile/kv/";
	private String currentFilePath = null;
	
	
	@Test
	public void testIntKey() throws Exception{
		for(int i=0;i<NATIVECONF.normalvalueclasses.length;i++){
			Job job = IntKeyMapper.getIntKeyTestJob(NATIVECONF.normalvalueclasses[i], inputFilePath);
		   	job.waitForCompletion(true);
		}
	}
	@Test
	public void testFloatKey() throws Exception{
		    Configuration conf = NATIVECONF.getNativeConf();
		    Job job = new Job(conf, "FloatKeytest");
		    job.setJarByClass(KVMappers.class);
		    job.setMapperClass(KVMappers.FloatKeyMapper.class);
		    job.setOutputKeyClass(FloatWritable.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(inputFilePath));
		    currentFilePath = outputDir+"Floatoutput";
		    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
		    job.waitForCompletion(true);
	}
	@Test
	public void testDoubleKey() throws Exception{
		    Configuration conf = NATIVECONF.getNativeConf();
		    Job job = new Job(conf, "DoubleKeytest");
		    job.setJarByClass(KVMappers.class);
		    job.setMapperClass(KVMappers.DoubleKeyMapper.class);
		    job.setOutputKeyClass(DoubleWritable.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(inputFilePath));
		    currentFilePath = outputDir+"Doubleoutput";
		    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
		    job.waitForCompletion(true);
	}
	@Test
	public void testBooleanKey() throws Exception{
	    Configuration conf = NATIVECONF.getNativeConf();
	    Job job = new Job(conf, "BooleanKeytest");
	    job.setJarByClass(KVMappers.class);
	    job.setMapperClass(KVMappers.BooleanKeyMapper.class);
	    job.setOutputKeyClass(BooleanWritable.class);
	    job.setOutputValueClass(BooleanWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputFilePath));
	    currentFilePath = outputDir+"Booleanoutput";
	    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
	    job.waitForCompletion(true);
	}
	@Test
	public void testLongKey() throws Exception{
	    Configuration conf = NATIVECONF.getNativeConf();
	    Job job = new Job(conf, "LongKeytest");
	    job.setJarByClass(KVMappers.class);
	    job.setMapperClass(KVMappers.LongKeyMapper.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputFilePath));
	    currentFilePath = outputDir+"Longoutput";
	    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
	    job.waitForCompletion(true);
	}
	@Test
	public void testObjectKey() throws Exception{
	    Configuration conf = NATIVECONF.getNativeConf();
	    Job job = new Job(conf, "ObjectKeytest");
	    job.setJarByClass(KVMappers.class);
	    job.setMapperClass(KVMappers.ObjectKeyMapper.class);
	    job.setOutputKeyClass(ObjectWritable.class);
	    job.setOutputValueClass(ObjectWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputFilePath));
	    currentFilePath = outputDir+"Objectoutput";
	    FileOutputFormat.setOutputPath(job, new Path(currentFilePath));
	    job.waitForCompletion(true);
	}
	@Before
	public void setup(){
		//delete output file on hadoop
		if(new File(outputDir).exists()){
			;
		}else{
			new File(outputDir).mkdirs();
		}
	}
	@After
    public void tearDown() {
        //
    }

}
