package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.nativetask.testframe.util.BytesUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVJob {
	public static final String FILE_INPUTPATH_CONF_KEY = "nativetask.kvtest.inputfile.path";
	public static final String FILE_OUTPUTPATH_CONF_KEY = "nativetask.kvtest.outputfile.path";
	Job job = null;

	public static class ValueMapper<KTYPE, VTYPE> extends
			Mapper<KTYPE, VTYPE, KTYPE, VTYPE> {
		public void map(KTYPE key, VTYPE value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class ValueMReducer<KTYPE, VTYPE> extends
			Reducer<KTYPE, VTYPE, KTYPE, VTYPE> {
		public void reduce(KTYPE key, VTYPE value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class ValueReducer<KTYPE, VTYPE> extends
			Reducer<KTYPE, VTYPE, KTYPE, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(KTYPE key, Iterable<VTYPE> values, Context context)
				throws IOException, InterruptedException {
			CRC32 valuecrc = new CRC32();
			for (VTYPE val : values) {
				valuecrc.update(BytesUtil.VTYPEToBytes(val));
			}
			result.set(valuecrc.getValue());
			context.write(key, result);
		}
	}

	public KVJob(String jobname, Configuration conf, Class<?> keyclass,
			Class<?> valueclass) throws Exception {
		// TODO Auto-generated constructor stub
		job = new Job(conf, jobname);
		job.setJarByClass(KVJob.class);
		job.setMapperClass(KVJob.ValueMapper.class);
		job.setOutputKeyClass(keyclass);
		job.setOutputValueClass(valueclass);

		String fileinputpath = conf.get(FILE_INPUTPATH_CONF_KEY, "");
		String fileoutputpath = conf.get(FILE_OUTPUTPATH_CONF_KEY, "");

		if (conf.get(KVTest.NATIVETASK_KVTEST_CONF_CREATEFILE).equals("true")) {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(fileinputpath));
			fs.close();

			TestFile testfile = new TestFile(Integer.valueOf(conf.get(
					TestFile.FILESIZE_CONF_KEY, "1000")), fileinputpath,
					keyclass.getName(), valueclass.getName());
			testfile.createSequenceTestFile();

		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(fileinputpath));
		FileOutputFormat.setOutputPath(job, new Path(fileoutputpath));
	}

	public void runJob() throws Exception {

		job.waitForCompletion(true);
	}
}
