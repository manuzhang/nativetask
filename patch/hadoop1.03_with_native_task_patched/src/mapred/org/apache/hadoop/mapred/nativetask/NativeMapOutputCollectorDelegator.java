package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskDelegation;
import org.apache.hadoop.mapred.nativetask.handlers.NativeCollectorOnlyHandler;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;

public class NativeMapOutputCollectorDelegator<K, V> implements TaskDelegation.MapOutputCollectorDelegator<K, V> {

  private static Log LOG = LogFactory.getLog(NativeMapOutputCollectorDelegator.class);
  private JobConf job;
  private NativeCollectorOnlyHandler handler;
  
  @Override
  public void collect(K key, V value, int partition) throws IOException,
      InterruptedException {
    handler.collect((Writable)key, (Writable)value, partition);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    handler.close();
  }

  @Override
  public void flush() throws IOException, InterruptedException,
      ClassNotFoundException {
    handler.flush();
  }

  @Override
  public void setConf(Configuration conf) {
    this.job = new JobConf(conf);
    
    if (job.getNumReduceTasks() == 0) {
      throw new RuntimeException("There is no reducer, no need to use native output collector");
    }
    if (job.getCombinerClass() != null) {
      throw new RuntimeException("Native output collector don't support java combiner" + 
          job.getCombinerClass().getName());
    }
    if (job.getClass("mapred.output.key.comparator.class", null,
        RawComparator.class) != null) {
      throw new RuntimeException("Native output collector don't support java comparator " + 
        RawComparator.class.getName());
    }
    if (job.getBoolean("mapred.compress.map.output", false) == true) {
      if (!"org.apache.hadoop.io.compress.SnappyCodec".equals(job
          .get("mapred.map.output.compression.codec"))) {
        throw new RuntimeException("Native output collector don't support compression codec other than snappy" + 
            job.get("mapred.map.output.compression.codec"));
      }
    }
    
    Class<?> keyCls = job.getMapOutputKeyClass();
    try {
      INativeSerializer serializer = NativeSerialization.getInstance().getSerializer(keyCls);
      if (null == serializer) {
        throw new RuntimeException("Cannot find serializer for ) " +
            keyCls.getName());
      }
      
      if (! (serializer instanceof INativeComparable)) {
        throw new RuntimeException("Native output collector don't support this key, this key is not comparable in native " + 
            keyCls.getName());
      }
    } catch (IOException e) {
      throw new RuntimeException("Cannot find serializer for ) " +
          keyCls.getName());
    }
    
    boolean ret = NativeRuntime.isNativeLibraryLoaded();
    if (ret) {
      NativeRuntime.configure(job);
    }
    else {
      throw new RuntimeException("Nativeruntime cannot be loaded, please check the libnativetask.so is in hadoop library dir");
    }
    
    this.handler = null;
    try {
      handler = new NativeCollectorOnlyHandler(job);
    }
    catch(IOException e) {
      throw new RuntimeException("Native output collector cannot be loaded", e);
    }
    
    LOG.info("Native output collector can be successfully enabled!");
  }

  @Override
  public Configuration getConf() {
    return this.job;
  }
}
