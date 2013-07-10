package org.apache.hadoop.mapred.nativetask.compresstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class CompressTest {
	public static final String NATIVETASK_COMPRESS_CONF_PATH = "test-compress-conf.xml";
	@Test
	public void testCompress() throws ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(CompressMapper.outputFileDir));
		fs.close();
		Job job = CompressMapper.getCompressJob(new Text());
		job.waitForCompletion(true);
	}
}
