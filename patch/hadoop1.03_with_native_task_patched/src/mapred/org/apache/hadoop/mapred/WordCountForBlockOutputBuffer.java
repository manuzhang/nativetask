package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountForBlockOutputBuffer {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, BytesWritable>{
    
    static byte[] ONE = new byte[4];
    static {
      toBytes(1, ONE);
    }
    private final static BytesWritable one = new BytesWritable(ONE);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,BytesWritable,Text,IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<BytesWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      System.out.println("The key is : " + key.toString());
      int sum = 0;
      for (BytesWritable val : values) {
        int current = toInt(val.get(), 0, 4);
        sum += current;
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
//  public static class Combiner 
//  extends Reducer<Text,BytesWritable,Text,BytesWritable> {
//
//    private BytesWritable result = new BytesWritable(new byte[4]);
//    
//    public void reduce(Text key, Iterable<BytesWritable> values, 
//                      Context context
//                      ) throws IOException, InterruptedException {
//     int sum = 0;
//     for (BytesWritable val : values) {
//       int current = toInt(val.get(), 0, 4);
//       sum += current;
//     }
//     toBytes(sum, result.get());
//     context.write(key, result);
//    }
//    }
  
  public static int toInt(byte[] bytes, int offset, final int length) {
    final int SIZEOF_INT = 4;
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw new RuntimeException(
          "toInt exception. length not equals to SIZE of Int or buffer overflow");
    }
    int ch1 = bytes[0] & 0xff;
    int ch2 = bytes[1] & 0xff;
    int ch3 = bytes[2] & 0xff;
    int ch4 = bytes[3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));

  }

  // same rule as DataOutputStream
  public static byte[] toBytes(int v, byte[] b) {
    b[0] = (byte) ((v >>> 24) & 0xFF);
    b[1] = (byte) ((v >>> 16) & 0xFF);
    b[2] = (byte) ((v >>> 8) & 0xFF);
    b[3] = (byte) ((v >>> 0) & 0xFF);
    return b;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCountForBlockOutputBuffer.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(Combiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
   // job.setNumReduceTasks(0);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}