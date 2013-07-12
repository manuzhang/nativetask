package org.apache.hadoop.mapred.nativetask.kvtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

public class GenTestFile {
	public static void createSequenceTestFile(String filepath, Class<?> keytype,
			Class<?> valuetype, int linenum) {
		SequenceFile.Writer writer = null;
		try {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			Path outputfilepath = new Path(filepath);
			writer = new SequenceFile.Writer(hdfs, conf, outputfilepath,
					keytype, valuetype);
			for (int i = 0; i < linenum; i++) {
				writer.append(generateData(keytype), generateData(valuetype));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	private static Object generateData(Class<?> dataclass) {
		try {
			return dataclass.newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
