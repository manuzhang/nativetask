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
package org.apache.hadoop.mapred.nativetask.utils;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

@SuppressWarnings({ "deprecation" })
public class TestBytesUtil extends TestCase {

  public void testBytesUtil() {

    final String str = "I am good!";
    final byte[] bytes = BytesUtil.toBytes(str);

    Assert.assertEquals(str, BytesUtil.fromBytes(bytes));

    final int a = 1000;
    final byte[] intBytes = new byte[4];
    BytesUtil.toBytes(a, intBytes);

    Assert.assertEquals(a, BytesUtil.toInt(intBytes));

    final byte[] largeBuffer = new byte[1000];
    BytesUtil.toBytes(a, largeBuffer, 100, 4);

    Assert.assertEquals(a, BytesUtil.toInt(largeBuffer, 100, 4));
  }
}
