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

public class Constants {

  /**
   * map task constants
   */
  public static final String MAP_SORT_CLASS = "map.sort.class";
  public static final String MAPRED_COMBINER_CLASS = "mapred.combiner.class";

  /**
   * configure device to send native logs to, the default is stderr
   */
  public static final String NATIVE_LOG_DEVICE = "native.log.device";
  /**
   * configuration for native side to check hadoop version
   */
  public static final String NATIVE_HADOOP_VERSION = "native.hadoop.version";

  /**
   * configuration to enable a native combiner
   */
  public static final String NATIVE_COMBINER_CLASS = "native.combiner.class";


  /**
   * configure buffer size to transfer data between java and native
   */
  public static final String NATIVE_PROCESSOR_BUFFER_KB = "native.processor.buffer.kb";
  public static int NATIVE_PROCESSOR_BUFFER_KB_DEFAULT = 64;
  public static int NATIVE_ASYNC_PROCESSOR_BUFFER_KB_DEFAULT = 1024;

  /**
   * configure the frequency that native side updates counters and reports to java
   */
  public static final String NATIVE_STATUS_UPDATE_INTERVAL = "native.update.interval";
  public static int NATIVE_STATUS_UPDATE_INTERVAL_DEFVAL = 3000;

  /**
   * configure whether to use native serialization or java writable serialization
   */
  public static final String SERIALIZATION_FRAMEWORK = "SerializationFramework";
  /**
   * serialization constants
   */
  public static int SIZEOF_PARTITION_LENGTH = 4;
  public static int SIZEOF_KEY_LENGTH = 4;
  public static int SIZEOF_VALUE_LENGTH = 4;
  public static int SIZEOF_KV_LENGTH = SIZEOF_KEY_LENGTH + SIZEOF_VALUE_LENGTH;

  /**
   * configuration to load custom native library
   * if you've implemented a native library (.so file) called CustomPlatform
   * the value should be "CustomPlatform=${path_to_native_library}
   */
  public static final String NATIVE_CLASS_LIBRARY_BUILDIN = "native.class.library.buildin";
  /**
   * configuration to load custom native key comparators
   * if you've implemented a native CustomComparator in CustomPlatform,
   * the value should be "CustomPlatform.CustomPlatform::CustomComparator
   */
  public static final String NATIVE_MAPOUT_KEY_COMPARATOR = "native.map.output.key.comparator";
}
