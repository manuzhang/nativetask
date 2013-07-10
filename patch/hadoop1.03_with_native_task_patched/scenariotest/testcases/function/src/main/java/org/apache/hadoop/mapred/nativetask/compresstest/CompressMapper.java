package org.apache.hadoop.mapred.nativetask.compresstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompressMapper {
	public static final String inputFile = "/compress/input.txt";
	public static final String outputFileDir = "/compress/output/";
	
	public static class TextCompressMapper extends Mapper<Object,Text,IntWritable,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new IntWritable(2345), value);
		}
	}
	public static<VType> Job getCompressJob(VType instance){
		Configuration conf = new Configuration();
		conf.addResource(CompressTest.NATIVETASK_COMPRESS_CONF_PATH);
		Job job = null;
		try{
			job = new Job(conf,"CompressMapperJob");
			job.setJarByClass(CompressMapper.class);
			job.setMapperClass(TextCompressMapper.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputFileDir
					+ instance.getClass().getName()));
		}catch(Exception e){
			e.printStackTrace();
		}
		return job;
	}
}
