package org.apache.hadoop.mapred.nativetask.handlers;

import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.serde.SerializationType;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.util.Progress;

public class CombinerHandler<K, V> extends NativeBatchProcessor<K, V, K, V>
    implements RawKeyValueIterator, OutputCollector<K, V>, ICombineHandler {

  private static Log LOG = LogFactory.getLog(NativeCollectorOnlyHandler.class);

  private JobConf jobConf;
  private static final byte[] REFILL = BytesUtil.toBytes("refill");

  byte[] keyBytes = new byte[0];
  byte[] valueBytes = new byte[0];
  int count = 0;

  DataInputBuffer keyBuffer = new DataInputBuffer();
  DataInputBuffer valueBuffer = new DataInputBuffer();

  private TaskReporter reporter;

  private TaskAttemptID taskAttemptID;

  private Counter combineInputCounter;

  int remains = 0;
  private boolean noMoreData = false;

  public CombinerHandler(Configuration conf, Class<K> iKClass,
      Class<V> iVClass, int inputBufferCapacity, int outputBufferCapacity,
      TaskReporter reporter, TaskAttemptID taskAttemptID) throws IOException {
    super(iKClass, iVClass, iKClass, iVClass, "NativeTask.CombineHandler",
        inputBufferCapacity, outputBufferCapacity);
    this.jobConf = new JobConf(conf);
    this.reporter = reporter;
    this.taskAttemptID = taskAttemptID;

    this.combineInputCounter = reporter.getCounter(COMBINE_INPUT_RECORDS);
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

  @Override
  public int combine() {
    CombinerRunner<K, V> combinerRunner;
    try {
      combinerRunner = CombinerRunner.create(jobConf, taskAttemptID,
          combineInputCounter, reporter, null);
      combinerRunner.combine(this, this);
      nativeWriter.close();
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return -1;
    }
  }

  @Override
  public DataInputBuffer getKey() throws IOException {
    return keyBuffer;
  }

  @Override
  public DataInputBuffer getValue() throws IOException {
    return valueBuffer;
  }

  @Override
  public boolean next() throws IOException {
    if (noMoreData) {
      return false;
    }

    if (remains == 0) {
      remains = loadData(SerializationType.Writable);
      if (remains == 0) {
        noMoreData = true;
        return false;
      }
      outputBuffer.position(0);
      outputBuffer.limit(remains);
    }

    int keyLength = nativeReader.readInt();
    if (keyBytes.length < keyLength) {
      keyBytes = new byte[keyLength];
    }
    nativeReader.read(keyBytes, 0, keyLength);

    int valueLength = nativeReader.readInt();
    if (valueBytes.length < valueLength) {
      valueBytes = new byte[valueLength];
    }
    nativeReader.read(valueBytes, 0, valueLength);

    keyBuffer.reset(keyBytes, keyLength);
    valueBuffer.reset(valueBytes, valueLength);

    remains -= keyLength + valueLength + 8;
    return true;
  }

  @Override
  public void collect(K key, V value) throws IOException {
    serializer.serializeKV(nativeWriter, (Writable) key, (Writable) value);
  }

  @Override
  public Progress getProgress() {
    // TODO Auto-generated method stub
    return null;
  }

  public static <K, V> ICombineHandler create(Configuration conf,
      Class<K> iKClass, Class<V> iVClass, int inputBufferCapacity,
      int outputBufferCapacity, TaskReporter reporter,
      TaskAttemptID taskAttemptID) throws IOException {

    String combinerClazz = conf.get("mapred.combiner.class");
    if (null == combinerClazz) {
      combinerClazz = conf.get("mapreduce.combine.class");
    }

    if (null == combinerClazz) {
      return null;
    } else {
      CombinerHandler handler = new CombinerHandler<K, V>(conf, iKClass,
          iVClass, inputBufferCapacity, outputBufferCapacity, reporter,
          taskAttemptID);
      handler.init(conf);
      return handler;
    }
  }

  @Override
  public long getId() {
    return getNativeHandler();
  }
}
