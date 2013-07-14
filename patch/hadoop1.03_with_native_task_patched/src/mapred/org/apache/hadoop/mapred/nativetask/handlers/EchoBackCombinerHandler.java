package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.serde.SerializationType;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

public class EchoBackCombinerHandler<K, V> extends NativeBatchProcessor<K, V, K, V>
implements ICombineHandler {
  
  private static Log LOG = LogFactory.getLog(NativeCollectorOnlyHandler.class);


  private JobConf jobConf;
  private static final byte[] REFILL = BytesUtil.toBytes("refill");
  Writable key;
  Writable value;
  
  public EchoBackCombinerHandler(Configuration conf, Class<K> iKClass, Class<V> iVClass,
      int inputBufferCapacity,
      int outputBufferCapacity) throws IOException {
    super(iKClass, iVClass, iKClass, iVClass, "NativeTask.CombineHandler",
        inputBufferCapacity, 
        outputBufferCapacity);
    this.jobConf = new JobConf(conf);
    try {
      key = (Writable)(iKClass.newInstance());
      value = (Writable)(iVClass.newInstance());
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
  }

  
  @Override
  protected byte[] sendCommandToJava(byte[] data) throws IOException {
    String cmd = BytesUtil.fromBytes(data);
    if (cmd.equals("combine")) {
      combine();
    }
    return null;
  }
  
  int loadData(SerializationType type) throws IOException {
    byte[] command = new byte[REFILL.length + 4];
    System.arraycopy(REFILL, 0, command, 0, REFILL.length);
    BytesUtil.toBytes(type.getType(), command, REFILL.length, 4);
    byte[] length = sendCommandToNative(command);
    return BytesUtil.toInt(length);
  }

  public int combine()  {
    
    try {
    //while (total > 0)  
    int total = 0;
    while(0 < (total = loadData(SerializationType.Native))) {
      outputBuffer.position(0);
      outputBuffer.limit(total);
      
      int read = 0;
      while(read < total) {
        int kvLength = deserializer.deserializeKV(nativeReader, key, value);
        read += kvLength + 8;
        serializer.serializeKV(nativeWriter, key, value);
      }
    }
    nativeWriter.close();
    }
    catch(IOException e) {
      LOG.info("exception in combine", e);
      return -1;
    }
    return 0;
  }


  @Override
  public long getId() {
    return getNativeHandler();
  }
}
