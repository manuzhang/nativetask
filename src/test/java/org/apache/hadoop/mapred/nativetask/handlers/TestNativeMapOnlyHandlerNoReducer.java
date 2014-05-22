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
package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.mockito.Matchers;
import org.mockito.Mockito;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestNativeMapOnlyHandlerNoReducer extends TestCase {
  
  private NativeMapOnlyHandlerNoReducer handler;
  private INativeHandler nativeHandler;
  private BufferPusher pusher;
  private BufferPushee pushee;

  @Override
  public void setUp() throws IOException {
    this.nativeHandler = Mockito.mock(INativeHandler.class);
    this.pusher = Mockito.mock(BufferPusher.class);
    this.pushee = Mockito.mock(BufferPushee.class);

    Mockito.when(nativeHandler.getInputBuffer()).thenReturn(new InputBuffer(BufferType.HEAP_BUFFER, 100));
  }

  public void testDataReceiverSetting() throws IOException {
    this.handler = new NativeMapOnlyHandlerNoReducer(nativeHandler, pusher, pushee);
    Mockito.verify(nativeHandler, Mockito.times(1)).setDataReceiver(Matchers.any(NativeMapOnlyHandlerNoReducer.class));
    Mockito.verify(nativeHandler, Mockito.times(1)).getInputBuffer();
  }

  public void testCollect() throws IOException {
    this.handler = new NativeMapOnlyHandlerNoReducer(nativeHandler, pusher, pushee);
    handler.collect(new BytesWritable(), new BytesWritable());
    handler.receiveData();
    handler.close();
    handler.close();

    Mockito.verify(pusher, Mockito.times(1)).collect(Matchers.any(BytesWritable.class),
        Matchers.any(BytesWritable.class));
    Mockito.verify(pushee, Mockito.times(1)).collect(Matchers.any(InputBuffer.class));

    Mockito.verify(pusher, Mockito.times(1)).close();
    Mockito.verify(pushee, Mockito.times(1)).close();
    Mockito.verify(nativeHandler, Mockito.times(1)).close();
  }
}
