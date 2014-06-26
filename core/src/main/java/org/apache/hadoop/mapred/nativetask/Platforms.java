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
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;

public class Platforms {

  private static final ServiceLoader<Platform> platforms = ServiceLoader.load(Platform.class);
  
  public static void init(Configuration conf) throws IOException {

    NativeSerialization.getInstance().reset();
    synchronized (platforms) {
      for (Platform platform : platforms) {
        platform.init();
      }
    }
  }

  public static boolean support(INativeSerializer serializer, JobConf job) {
    boolean supported;
    synchronized (platforms) {
      for (Platform platform : platforms) {
        supported = platform.support(serializer, job);
        if (supported) {
          return true;
        }
      }
    }
    return false;
  }
}
