package org.apache.hadoop.mapred.nativetask;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeReduceTaskDelegator;

public class NATIVECONF {
	public static Class<?>[] normalvalueclasses={BooleanWritable.class,BytesWritable.class,
		ByteWritable.class,DoubleWritable.class,FloatWritable.class,IntWritable.class,
		LongWritable.class,NullWritable.class,Text.class,UTF8.class,VIntWritable.class,
		VLongWritable.class};
	public static Configuration getNativeConf(){
		Configuration conf = new Configuration();
		conf.set("mapreduce.map.output.collector.delegator.class","org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator");
		conf.set("native.task.enabled","true");
		conf.set(Constants.MAPRED_REDUCETASK_DELEGATOR_CLASS,
					NativeReduceTaskDelegator.class.getCanonicalName());
		conf.set("native.recordwriter.class","NativeTask.TextIntRecordWriter");
		return conf;
	}
}
