package org.apache.hadoop.mapred.nativetask.combinertest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.IntSumReducer;
import org.apache.hadoop.mapred.nativetask.combinertest.WordCount.TokenizerMapper;
import org.apache.hadoop.mapred.nativetask.kvtest.TestFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerJobFactory {
	public static final String NATIVETASK_COLLECTOR_DELEGATOR = "mapreduce.map.output.collector.delegator.class";
	public static final String NATIVETASK_COLLECTOR_DELEGATOR_CLASS = "org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator";
	public static final String COMBINER_CONF_SOURCE = "test-combiner-conf.xml";
	public static final String NATIVETASK_TEST_COMBINER_INPUTPATH_KEY = "nativetask.combinertest.inputpath";
	public static final String NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV = "./combinertest/input";
	public static final String NATIVETASK_TEST_COMBINER_OUTPUTPATH_KEY = "nativetask.combinertest.outputdir";
	public static final String NATIVETASK_TEST_COMBINER_OUTPUTPATH_DEFAULTV = "./combinertest/output/native";
	public static final String NORMAL_TEST_COMBINER_OUTPUTPATH_KEY = "normal.combinertest.outputdir";
	public static final String NORMAL_TEST_COMBINER_OUTPUTPATH_DEFAULTV = "./combinertest/output/normal";

	public static Job getWordCountNativeJob(String jobname) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(COMBINER_CONF_SOURCE);
		conf.set(NATIVETASK_COLLECTOR_DELEGATOR,
				NATIVETASK_COLLECTOR_DELEGATOR_CLASS);
		String inputpath = conf.get(
				NATIVETASK_TEST_COMBINER_INPUTPATH_KEY,
				NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV)
				+ "/wordcount";
		String outputpath = conf.get(
				NATIVETASK_TEST_COMBINER_OUTPUTPATH_KEY,
				NATIVETASK_TEST_COMBINER_OUTPUTPATH_DEFAULTV)
				+ "/" + jobname;
		FileSystem fs = FileSystem.get(conf);
		// no such file ,then create it
		if (!fs.exists(new Path(inputpath))) {
			new TestFile(conf.getInt("nativetask.combiner.wordcount.filesize",
					10000), inputpath, Text.class.getName(),
					Text.class.getName()).createSequenceTestFile();

		}
		Job job = new Job(conf, jobname);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
		return job;
	}

	public static Job getWordCountNormalJob(String jobname) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(COMBINER_CONF_SOURCE);
		Job job = new Job(conf, jobname);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(
				job,
				new Path(conf.get(NATIVETASK_TEST_COMBINER_INPUTPATH_KEY,
						NATIVETASK_TEST_COMBINER_INPUTPATH_DEFAULTV)
						+ "/wordcount"));
		FileOutputFormat.setOutputPath(
				job,
				new Path(conf.get(NORMAL_TEST_COMBINER_OUTPUTPATH_KEY,
						NORMAL_TEST_COMBINER_OUTPUTPATH_DEFAULTV)
						+ "/"
						+ jobname));
		return job;
	}

	public static Job getCombinerTestNativeJob(String jobname,
			Class<?> mapperCls, Class<?> combinerCls, Class<?> reducerCls) {
		return null;
	}
}
