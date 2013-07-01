package org.apache.hadoop.mapred.nativetask.kvtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeReduceTaskDelegator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import org.apache.hadoop.mapred.nativetask.*;

import static org.junit.Assert.*;

public class KVTest extends TestCase {
	private static String inputFilePath = "/f1.txt";
	private static String outputFilePath = "/out23";
	@Test
	public void testIntKey(){
		try{
			Configuration conf = NATIVECONF.getNativeConf();
			Job job = new Job(conf, "IntKeytest");
		    job.setJarByClass(KVMappers.class);
		    job.setMapperClass(KVMappers.IntKeyMapper.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(inputFilePath));
		    FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		    job.waitForCompletion(true);
		}catch (Exception e){
			;
		}
	}
	@Before
	public void setup(){
		//delete output file on hadoop
	}
	@After
    public void tearDown() {
        //
    }

}
