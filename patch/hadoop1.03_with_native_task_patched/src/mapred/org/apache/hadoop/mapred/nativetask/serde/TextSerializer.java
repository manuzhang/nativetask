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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

public class TextSerializer implements INativeSerializer<Text>, INativeComparable  {
  private Method setCapacity = null;

  public TextSerializer() throws SecurityException, NoSuchMethodException {
    setCapacity = Text.class.getDeclaredMethod("setCapacity", int.class,
        boolean.class);
    setCapacity.setAccessible(true);
  }

  @Override
  public int getLength(Text w) throws IOException {
    return w.getLength();
  }

  @Override
  public void serialize(Text w, DataOutput out) throws IOException {
    out.write(w.getBytes(), 0, w.getLength());
  }

  @Override
  public void deserialize(Text w, int length, DataInput in) throws IOException {

    try {
      setCapacity.invoke(w, length, false);
    } catch (Exception e) {
      throw new IOException(e);
    }
    byte[] bytes = w.getBytes();
    in.readFully(bytes, 0, length);
  }
}
