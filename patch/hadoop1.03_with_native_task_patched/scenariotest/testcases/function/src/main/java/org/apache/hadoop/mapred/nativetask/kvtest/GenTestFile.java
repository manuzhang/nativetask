package org.apache.hadoop.mapred.nativetask.kvtest;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenTestFile {
	public static void createHDFSintFile(String genfilepath, Class<?> keytype,
			Class<?> valuetype, int lineNum) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FSDataOutputStream os = hdfs.create(new Path(genfilepath));
		Random r = new Random();
		for (int i = 0; i < lineNum; i++) {
			String linecontent = keytype.newInstance().toString() + "\t"
					+ valuetype.newInstance().toString() + "\n";
			os.write(linecontent.getBytes("utf-8"));
		}
		os.close();
		hdfs.close();
	}
}
