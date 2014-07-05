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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;

public abstract class Platform {
  private final NativeSerialization serialization;
  protected Set<String> keyClassNames = new HashSet<String>();

  public Platform() {
    this.serialization = NativeSerialization.getInstance();
  }

  public abstract void init() throws IOException;

  public abstract String name();

  protected void registerKey(String keyClassName, Class key) throws IOException {
    serialization.register(keyClassName, key);
    keyClassNames.add(keyClassName);
  }
  
  protected abstract boolean support(String keyClassName, INativeSerializer serializer, JobConf job);

  protected abstract boolean define(Class keyComparator);
}
