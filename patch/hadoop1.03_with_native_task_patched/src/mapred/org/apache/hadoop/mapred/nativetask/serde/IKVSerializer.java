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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.NativeDataReader;
import org.apache.hadoop.mapred.nativetask.NativeDataWriter;

/**
 * 
 * 
 *
 */
public interface IKVSerializer {

  public int serializeKV(NativeDataWriter out, Writable key, Writable value)
      throws IOException;

  public int serializeKV(NativeDataWriter out, int remain, Writable key,
      Writable value) throws IOException;

  public int deserializeKV(NativeDataReader in, Writable key, Writable value)
      throws IOException;
}
