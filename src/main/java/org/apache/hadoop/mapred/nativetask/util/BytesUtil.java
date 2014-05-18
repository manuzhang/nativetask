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

package org.apache.hadoop.mapred.nativetask.util;

import java.io.UnsupportedEncodingException;

public class BytesUtil {

  public static byte[] toBytes(String str) {
    if (str == null) {
      return null;
    }
    try {
      return str.getBytes("utf-8");
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String fromBytes(byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return new String(data, "utf-8");
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, 4);
  }

  public static int toInt(byte[] bytes, int offset, final int length) {
    final int SIZEOF_INT = 4;
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw new RuntimeException("toInt exception. length not equals to SIZE of Int or buffer overflow");
    }
    final int ch1 = bytes[offset] & 0xff;
    final int ch2 = bytes[offset + 1] & 0xff;
    final int ch3 = bytes[offset + 2] & 0xff;
    final int ch4 = bytes[offset + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));

  }

  // same rule as DataOutputStream
  public static void toBytes(int v, byte[] b) {
    b[0] = (byte) ((v >>> 24) & 0xFF);
    b[1] = (byte) ((v >>> 16) & 0xFF);
    b[2] = (byte) ((v >>> 8) & 0xFF);
    b[3] = (byte) ((v >>> 0) & 0xFF);
    return;
  }

  // same rule as DataOutputStream
  public static void toBytes(int v, byte[] b, int offset, int length) {
    b[offset] = (byte) ((v >>> 24) & 0xFF);
    b[offset + 1] = (byte) ((v >>> 16) & 0xFF);
    b[offset + 2] = (byte) ((v >>> 8) & 0xFF);
    b[offset + 3] = (byte) ((v >>> 0) & 0xFF);
    return;
  }

}
