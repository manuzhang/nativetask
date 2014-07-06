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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.BytesWritableSerializer;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.log4j.Logger;

public class HivePlatform extends Platform {

  private static final Logger LOG = Logger.getLogger(HivePlatform.class);

  public HivePlatform() {
  }

  @Override
  public void init() throws IOException {
    registerKey("org.apache.hadoop.hive.ql.io.HiveKey", BytesWritableSerializer.class);
    LOG.info("Hive platform inited");
  }

  @Override
  public String name() {
    return "Hive";
  }

  @Override
  public boolean support(String keyClassName, INativeSerializer serializer, JobConf job) {
    if (keyClassNames.contains(keyClassName) && serializer instanceof INativeComparable) {
      String nativeComparator = Constants.NATIVE_MAPOUT_KEY_COMPARATOR + "." + keyClassName;
      job.set(nativeComparator, "HivePlatform.NativeObjectFactory::BytesComparator");
      if (job.get(Constants.NATIVE_CLASS_LIBRARY_BUILDIN) == null) {
        job.set(Constants.NATIVE_CLASS_LIBRARY_BUILDIN, "HivePlatform=libnativetaskhive.so");
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean define(Class comparatorClass) {
    return false;
  }
}
