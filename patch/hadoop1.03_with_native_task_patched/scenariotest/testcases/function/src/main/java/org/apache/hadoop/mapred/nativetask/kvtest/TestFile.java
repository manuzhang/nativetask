package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

public class TestFile {
	public static final int BytesMinLen = 8;// match to generate Double and Long
											// datatype
	public static final String FILESIZE_CONF_KEY = "kvtest.file.size";
	private byte[] databuf = null;
	private String keyClsName, valueClsName;
	private int filesize = 0;
	private int keyMaxBytesNum, keyMinBytesNum;
	private int valueMaxBytesNum, valueMinBytesNum;
	private SequenceFile.Writer writer = null;
	Random r = new Random();
	public static final int DATABUFSIZE = 1 << 22; // 4M

	private enum State {
		KEY, VALUE
	};

	public TestFile(int filesize, String filepath, String keytype,
			String valuetype) throws Exception {
		this.filesize = filesize;
		Class<?> tmpkeycls, tmpvaluecls;
		try {
			tmpkeycls = Class.forName(keytype);
			this.keyClsName = keytype;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			throw new Exception("key class not found: ", e);
		}
		try {
			tmpvaluecls = Class.forName(valuetype);
			this.valueClsName = valuetype;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			throw new Exception("key class not found: ", e);
		}
		this.databuf = new byte[DATABUFSIZE];
		try {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			Path outputfilepath = new Path(filepath);
			writer = new SequenceFile.Writer(hdfs, conf, outputfilepath,
					tmpkeycls, tmpvaluecls);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (keytype.equals(BooleanWritable.class.getName())) {
			keyMaxBytesNum = 1;
			keyMinBytesNum = 1;
		} else if (keytype.equals(DoubleWritable.class.getName())) {
			keyMaxBytesNum = 8;
			keyMinBytesNum = 8;
		} else if (keytype.equals(LongWritable.class.getName())) {
			keyMaxBytesNum = 8;
			keyMinBytesNum = 8;
		} else if (keytype.equals(VLongWritable.class.getName())) {
			keyMaxBytesNum = 8;
			keyMinBytesNum = 8;
		} else if (keytype.equals(Text.class.getName())) {
			keyMaxBytesNum = 64;
			keyMinBytesNum = 1;
		} else if (keytype.equals(UTF8.class.getName())) {
			keyMaxBytesNum = 64;
			keyMinBytesNum = 1;
		} else if (keytype.equals(ImmutableBytesWritable.class.getName())) {
			keyMaxBytesNum = 64;
			keyMinBytesNum = 1;
		} else {
			keyMaxBytesNum = 4;
			keyMinBytesNum = 4;
		}
		if (valuetype.equals(BooleanWritable.class.getName())) {
			valueMaxBytesNum = 1;
			valueMinBytesNum = 1;
		} else if (valuetype.equals(DoubleWritable.class.getName())) {
			valueMaxBytesNum = 8;
			valueMinBytesNum = 8;
		} else if (valuetype.equals(LongWritable.class.getName())) {
			valueMaxBytesNum = 8;
			valueMinBytesNum = 8;
		} else if (valuetype.equals(VLongWritable.class.getName())) {
			valueMaxBytesNum = 8;
			valueMinBytesNum = 8;
		} else if (valuetype.equals(Text.class.getName())) {
			valueMaxBytesNum = 64;
			valueMinBytesNum = 1;
		} else if (valuetype.equals(UTF8.class.getName())) {
			valueMaxBytesNum = 64;
			valueMinBytesNum = 1;
		} else if (valuetype.equals(ImmutableBytesWritable.class.getName())) {
			valueMaxBytesNum = 64;
			valueMinBytesNum = 1;
		} else {
			valueMaxBytesNum = 4;
			valueMinBytesNum = 4;
		}
	}

	public void createSequenceTestFile() throws Exception {
		int tmpfilesize = this.filesize - DATABUFSIZE;
		while (tmpfilesize > 0) {
			r.nextBytes(databuf);
			flushBuf(DATABUFSIZE);
			tmpfilesize -= DATABUFSIZE;
		}
		System.out.println("last buf");
		r.nextBytes(databuf);
		System.out.println("random finished");
		flushBuf(tmpfilesize + DATABUFSIZE);
		System.out.println("last buf flushed");
		if (writer != null)
			IOUtils.closeStream(writer);
		else
			throw new Exception("no writer to create sequenceTestFile!");
	}

	private void flushBuf(int buflen) throws Exception {
		// TODO Auto-generated method stub
		State state = State.KEY;
		Random r = new Random();
		int keybytesnum, valuebytesnum;
		keybytesnum = keyMaxBytesNum == keyMinBytesNum ? keyMaxBytesNum : (r
				.nextInt() % keyMaxBytesNum);
		keybytesnum = keybytesnum >= keyMinBytesNum ? keybytesnum
				: (keybytesnum % (keyMaxBytesNum - keyMinBytesNum) + keyMinBytesNum);
		valuebytesnum = valueMaxBytesNum == valueMinBytesNum ? valueMaxBytesNum
				: (r.nextInt() % valueMaxBytesNum);
		valuebytesnum = keybytesnum >= valueMinBytesNum ? valuebytesnum
				: (valuebytesnum % (valueMaxBytesNum - valueMinBytesNum) + valueMinBytesNum);
		System.out.println("begin to write keybytesnum,valuebytesnum,buflen"
				+ keybytesnum + ":" + valuebytesnum + " " + buflen);
		byte[] key = new byte[keybytesnum];
		byte[] value = new byte[valuebytesnum];
		for (int offset = 0; offset < buflen;) {
			if (state.equals(State.KEY)) {
				for (int i = 0; i < keybytesnum; i++)
					key[i] = databuf[i + offset];
				offset += keybytesnum;
				state = State.VALUE;
			} else {
				for (int i = 0; i < valuebytesnum; i++)
					value[i] = databuf[i + offset];
				offset += valuebytesnum;
				state = State.KEY;
				try {
					writer.append(generateData(key, this.keyClsName),
							generateData(value, this.valueClsName));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					throw new Exception("sequence file create failed", e);
				}
			}
		}
	}

	private Object generateData(byte[] bytes, String className) {
		// System.out.println("generate data " + bytes + " byte size: "
		// + bytes.length + " " + className);
		if (className.equals(IntWritable.class.getName())) {
			return new IntWritable(Bytes.toInt(bytes));
		} else if (className.equals(FloatWritable.class.getName())) {
			return new FloatWritable(r.nextFloat());
		} else if (className.equals(DoubleWritable.class.getName())) {
			return new DoubleWritable(r.nextDouble());
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
		} else if (className.equals(ImmutableBytesWritable.class.getName())) {
			return new ImmutableBytesWritable(bytes);
		} else if (className.equals(UTF8.class.getName())) {
			return new UTF8(Bytes.toString(bytes));
		} else
			return null;
	}
}
