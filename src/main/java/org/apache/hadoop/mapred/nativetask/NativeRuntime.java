/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;
import org.apache.hadoop.util.VersionInfo;

/**
 * This class stands for the native runtime It has three functions: 1. Create
 * native handlers for map, reduce, outputcollector, and etc 2. Configure native
 * task with provided MR configs 3. Provide file system api to native space, so
 * that it can use File system like HDFS.
 * 
 */
public class NativeRuntime {
  private static Log LOG = LogFactory.getLog(NativeRuntime.class);
  private static boolean nativeLibraryLoaded = false;

  private static Configuration conf = new Configuration();

  static {
    try {
      System.loadLibrary("nativetask");
      LOG.info("Nativetask JNI library loaded.");
      nativeLibraryLoaded = true;
    } catch (Throwable t) {
      // Ignore failures
      LOG.info("Failed to load nativetask JNI library with error: " + t);
      LOG.info("java.library.path=" + System.getProperty("java.library.path"));
      LOG.info("LD_LIBRARY_PATH=" + System.getenv("LD_LIBRARY_PATH"));
    }
  }

  // All configs that native side needed
  private static String[] usefulExternalConfigsKeys = {
      "mapred.map.tasks",
      "mapred.reduce.tasks", 
      "mapred.task.partition",
      "mapred.mapoutput.key.class", 
      "mapred.mapoutput.value.class",
      "mapred.output.key.class", 
      "mapred.output.value.class",
      "mapred.input.format.class",
      "mapred.output.format.class",
      "mapred.work.output.dir", 
      "mapred.textoutputformat.separator",
      "mapred.compress.map.output", 
      "mapred.map.output.compression.codec",
      "mapred.output.compress", 
      "mapred.output.compression.codec",
      "mapred.map.output.sort", 
      "io.sort.mb", 
      "io.file.buffer.size",
      "fs.default.name", 
      "fs.defaultFS" 
   };

  private static void assertNativeLibraryLoaded() {
    if (!nativeLibraryLoaded) {
      throw new RuntimeException("Native runtime library not loaded");
    }
  }

  public static boolean isNativeLibraryLoaded() {
    return nativeLibraryLoaded;
  }

  public static void configure(Configuration jobConf) {
    assertNativeLibraryLoaded();
    conf = jobConf;
    List<byte[]> nativeConfigs = new ArrayList<byte[]>();
    // add needed external configs
    for (int i = 0; i < usefulExternalConfigsKeys.length; i++) {
      String key = usefulExternalConfigsKeys[i];
      String value = conf.get(key);
      if (value != null) {
        // String newValue = getCompatibleValue(key, value);
        // if (newValue != null) {
        // value = newValue;
        // }
        //
        nativeConfigs.add(BytesUtil.toBytes(key));
        nativeConfigs.add(BytesUtil.toBytes(value));
      }
    }
    // add native.* configs
    for (Map.Entry<String, String> e : conf) {
      if (e.getKey().startsWith("native.")) {
        nativeConfigs.add(BytesUtil.toBytes(e.getKey()));
        nativeConfigs.add(BytesUtil.toBytes(e.getValue()));
      }
    }
    nativeConfigs.add(BytesUtil.toBytes("native.hadoop.version"));
    nativeConfigs.add(BytesUtil.toBytes(VersionInfo.getVersion()));
    JNIConfigure(nativeConfigs.toArray(new byte[nativeConfigs.size()][]));
  }

  public static void configure(String key, String value) {
    configure(key, BytesUtil.toBytes(value));
  }

  public static void configure(String key, boolean value) {
    configure(key, Boolean.toString(value));
  }

  public static void configure(String key, int value) {
    configure(key, Integer.toString(value));
  }

  public static void configure(String key, byte[] value) {
    assertNativeLibraryLoaded();
    byte[][] jniConfig = new byte[2][];
    jniConfig[0] = BytesUtil.toBytes(key);
    jniConfig[1] = value;
    JNIConfigure(jniConfig);
  }

  /**
   * create native object We use it to create native handlers
   * 
   * @param clazz
   * @return
   */
  public synchronized static long createNativeObject(String clazz) {
    assertNativeLibraryLoaded();
    long ret = JNICreateNativeObject(BytesUtil.toBytes(clazz));
    if (ret == 0) {
      LOG.warn("Can't create NativeObject for class " + clazz
          + ", prabobly not exist.");
    }
    return ret;
  }

  /**
   * Register a customized library
   * @param clazz
   * @return
   */
  public synchronized static long registerLibrary(String libraryName, String clazz) {
    assertNativeLibraryLoaded();
    long ret = JNIRegisterModule(BytesUtil.toBytes(libraryName), BytesUtil.toBytes(clazz));
    if (ret == 0) {
      LOG.warn("Can't create NativeObject for class " + clazz
          + ", prabobly not exist.");
    }
    return ret;
  }
  
  /**
   * destroy native object We use to destory native handlers
   */
  public synchronized static void releaseNativeObject(long addr) {
    assertNativeLibraryLoaded();
    JNIReleaseNativeObject(addr);
  }

  /**
   * Get the status report from native space
   * 
   * @param reporter
   * @throws IOException
   */
  public static void reportStatus(TaskReporter reporter) throws IOException {
    assertNativeLibraryLoaded();
    synchronized (reporter) {
      byte[] statusBytes = JNIUpdateStatus();
      DataInputBuffer ib = new DataInputBuffer();
      ib.reset(statusBytes, statusBytes.length);
      FloatWritable progress = new FloatWritable();
      progress.readFields(ib);
      reporter.setProgress(progress.get());
      Text status = new Text();
      status.readFields(ib);
      if (status.getLength() > 0) {
        reporter.setStatus(status.toString());
      }
      IntWritable numCounters = new IntWritable();
      numCounters.readFields(ib);
      if (numCounters.get() == 0) {
        return;
      }
      Text group = new Text();
      Text name = new Text();
      LongWritable amount = new LongWritable();
      for (int i = 0; i < numCounters.get(); i++) {
        group.readFields(ib);
        name.readFields(ib);
        amount.readFields(ib);
        reporter.incrCounter(group.toString(), name.toString(), amount.get());
      }
    }
  }

  /*******************************************************
   *** The following are file system api so that we can use HDFS from native
   ********************************************************/

  /**
   * Open File to read, used by native side We need this so that we are able to
   * read data from HDFS in native
   * 
   * @param pathUTF8
   *          file path
   * @return
   * @throws IOException
   */
  public static FSDataInputStream openFile(byte[] pathUTF8) throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.open(path);
  }

  /**
   * Create file to write, use by native side We need it so that we are able to
   * write data into HDFS in native side.
   * 
   * @param pathUTF8
   * @param overwrite
   * @return
   * @throws IOException
   */
  public static FSDataOutputStream createFile(byte[] pathUTF8, boolean overwrite)
      throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.create(path, overwrite);
  }

  /**
   * We need this so that we are able to get the HDFS file length in native
   * 
   * @param pathUTF8
   * @return
   * @throws IOException
   */
  public static long getFileLength(byte[] pathUTF8) throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.getLength(path);
  }

  /**
   * We need this so that we are able to check the HDFS file status
   * 
   */
  public static boolean exists(byte[] pathUTF8) throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.exists(path);
  }

  /**
   * We need this so that we are able to remove the file from HDFS
   */
  public static boolean remove(byte[] pathUTF8) throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    return fs.delete(path, true);
  }

  /**
   * We need this so that we are able to mkdirs in HDFS from native side
   */
  public static boolean mkdirs(byte[] pathUTF8) throws IOException {
    String pathStr = BytesUtil.fromBytes(pathUTF8);
    Path path = new Path(pathStr);
    FileSystem fs = path.getFileSystem(conf);
    boolean ret = fs.mkdirs(path);
    return ret;
  }

  /*******************************************************
   *** The following are JNI apis
   ********************************************************/

  /**
   * Config the native runtime with mapreduce job configurations.
   * 
   * @param configs
   */
  private native static void JNIConfigure(byte[][] configs);

  /**
   * create a native object in native space
   * 
   * @param clazz
   * @return
   */
  private native static long JNICreateNativeObject(byte[] clazz);

  /**
   * create the default native object for certain type
   * 
   * @param type
   * @return
   */
  @Deprecated
  private native static long JNICreateDefaultNativeObject(byte[] type);

  /**
   * destroy native object in native space
   * 
   * @param addr
   */
  private native static void JNIReleaseNativeObject(long addr);

  /**
   * get status update from native side Encoding: progress:float status:Text
   * Counter number: int the count of the counters Counters: array [group:Text,
   * name:Text, incrCount:Long]
   * 
   * @return
   */
  private native static byte[] JNIUpdateStatus();

  /**
   * Not used.
   */
  private native static void JNIRelease();

  /**
   * Not used.
   */
  private native static int JNIRegisterModule(byte[] path, byte[] name);
}
