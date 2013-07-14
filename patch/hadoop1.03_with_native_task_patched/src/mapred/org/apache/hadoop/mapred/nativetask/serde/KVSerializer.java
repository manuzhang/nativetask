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

public class KVSerializer<K, V> implements IKVSerializer {

  private INativeSerializer<Writable> keySerializer;
  private INativeSerializer<Writable> valueSerializer;

  // reserve some space for meta data.
  // Like partition id
  private final static int RESERVE_SPACE_FOR_META = 16; // bytes
  private final static int LENGTH_INT_BYTES = 4; // 4 bytes

  public KVSerializer(Class<K> kclass, Class<V> vclass) throws IOException {
    this.keySerializer = NativeSerialization.getInstance()
        .getSerializer(kclass);
    this.valueSerializer = NativeSerialization.getInstance().getSerializer(
        vclass);
  }

  @Override
  public int serializeKV(NativeDataWriter out, Writable key, Writable value)
      throws IOException {
    return serializeKV(out, -1, key, value);
  }

  /**
   * 
   * @param out
   * @param remain
   *          , -1 means unlimitted
   * @param key
   * @param value
   * @return
   * @throws IOException
   */
  @Override
  public int serializeKV(NativeDataWriter out, int remain, Writable key,
      Writable value) throws IOException {
    int keylength = keySerializer.getLength(key);
    int valueLength = valueSerializer.getLength(value);

    int kvLength = keylength + valueLength + LENGTH_INT_BYTES
        + LENGTH_INT_BYTES;

    if (-1 != remain) {
      if( remain < kvLength + RESERVE_SPACE_FOR_META) {
        return 0;
      }
    }
    else {
      int reserved = out.reserve(kvLength + RESERVE_SPACE_FOR_META);
      if (kvLength > reserved) {
        return 0;
      }
    }

    out.writeInt(keylength);

    keySerializer.serialize(key, out);

    out.writeInt(valueLength);

    valueSerializer.serialize(value, out);

    return keylength + valueLength + 8;
  }

  @Override
  public int deserializeKV(NativeDataReader in, Writable key, Writable value)
      throws IOException {
    int keyLength = in.readInt();
    keySerializer.deserialize(key, keyLength, in);

    int valueLength = in.readInt();
    valueSerializer.deserialize(value, valueLength, in);

    return keyLength + valueLength;
  }
}
