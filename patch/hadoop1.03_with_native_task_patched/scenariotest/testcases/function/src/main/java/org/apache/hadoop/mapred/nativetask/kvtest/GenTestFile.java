package org.apache.hadoop.mapred.nativetask.kvtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

public class GenTestFile {
	public static final int BytesMinLen = 8;//match to generate Double and Long datatype
	public static void createSequenceTestFile(String filepath,
			Class<?> keytype, Class<?> valuetype, int linenum) {
		SequenceFile.Writer writer = null;
		try {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			Path outputfilepath = new Path(filepath);
			writer = new SequenceFile.Writer(hdfs, conf, outputfilepath,
					keytype, valuetype);
			for (int i = 0; i < linenum; i++) {
				writer.append(generateData(keytype.getName()),
						generateData(valuetype.getName()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	private static Object generateData(String className) {
		byte[] bytes = BytesToAllTypes.generateBytes(BytesMinLen);
		if (className.equals(IntWritable.class.getName())) {
			return new IntWritable(Bytes.toInt(bytes));
		} else if (className.equals(FloatWritable.class.getName())) {
			return new FloatWritable(Bytes.toFloat(bytes));
		} else if (className.equals(DoubleWritable.class.getName())) {
			return new DoubleWritable(Bytes.toDouble(bytes));
		} else if (className.equals(LongWritable.class.getName())) {
			return new LongWritable(Bytes.toLong(bytes));
		} else if (className.equals(VIntWritable.class.getName())) {
			return new VIntWritable(Bytes.toInt(bytes));
		} else if (className.equals(VLongWritable.class.getName())) {
			return new VLongWritable(Bytes.toLong(bytes));
		} else if (className.equals(BooleanWritable.class.getName())) {
			return new BooleanWritable(bytes[0] % 2 == 1 ? true : false);
		} else if (className.equals(Text.class.getName())) {
			return new Text(Bytes.toString(bytes));
		} else if (className.equals(ByteWritable.class.getName())) {
			return new ByteWritable(bytes.length > 0 ? bytes[0] : 0);
		} else if (className.equals(BytesWritable.class.getName())) {
			return new BytesWritable(bytes);
		} else
			return null;
	}
}
