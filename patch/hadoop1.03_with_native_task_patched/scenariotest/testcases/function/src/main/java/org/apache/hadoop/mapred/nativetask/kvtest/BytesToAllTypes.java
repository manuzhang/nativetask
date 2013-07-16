package org.apache.hadoop.mapred.nativetask.kvtest;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

public class BytesToAllTypes {

	public static Object byteToObject(String className) {
		byte[] bytes = generateBytes(5);
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
			return new BooleanWritable(bytes[0]%2==1?true:false);
		} else if (className.equals(Text.class.getName())) {
			return new Text(Bytes.toString(bytes));
		} else if (className.equals(ByteWritable.class.getName())) {
			return new ByteWritable(bytes.length > 0 ? bytes[0] : 0);
		} else if (className.equals(BytesWritable.class.getName())) {
			return new BytesWritable(bytes);
		} else
			return null;
	}

	public static byte[] generateBytes(int len) {
		Random r = new Random();
		byte[] ret = new byte[len];
		r.nextBytes(ret);
		return ret;
	}
}
