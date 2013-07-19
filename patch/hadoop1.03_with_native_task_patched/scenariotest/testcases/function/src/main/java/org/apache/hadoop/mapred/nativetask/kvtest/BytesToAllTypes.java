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

	public static byte[] generateBytes(int len) {
		Random r = new Random();
		byte[] ret = new byte[len];
		r.nextBytes(ret);
		return ret;
	}
}
