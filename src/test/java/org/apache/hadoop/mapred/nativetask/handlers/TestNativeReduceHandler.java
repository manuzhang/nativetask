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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.mockito.Matchers;
import org.mockito.Mockito;

@SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
public class TestNativeReduceHandler extends TestCase {

  private NativeReduceTask handler;
  private BufferPullee mockLoader;
  private INativeHandler mockNativeHandler;
  private BufferPushee mockWriter;

  @Override
  public void setUp() throws IOException {

    this.mockLoader = Mockito.mock(BufferPullee.class);
    this.mockNativeHandler = Mockito.mock(INativeHandler.class);
    this.mockWriter = Mockito.mock(BufferPushee.class);
  }

  public void testRunNoJavaWriter() throws IOException {

    this.handler = new NativeReduceTask(mockNativeHandler, null, mockLoader);

    Mockito.when(mockNativeHandler.call(NativeReduceTask.RUN, null)).thenReturn(null);

    handler.run();
    handler.close();

    // flush once, write 4 int, and 2 byte array
    Mockito.verify(mockNativeHandler, Mockito.times(1)).call(NativeReduceTask.RUN, null);
    Mockito.verify(mockNativeHandler, Mockito.times(1)).close();
    Mockito.verify(mockLoader, Mockito.times(1)).close();
  }

  public void testRunJavaWriter() throws IOException {

    this.handler = new NativeReduceTask(mockNativeHandler, mockWriter, mockLoader);

    Mockito.when(mockNativeHandler.call(NativeReduceTask.RUN, null)).thenReturn(null);

    handler.run();
    handler.receiveData();
    handler.close();
    handler.close();
    
    // flush once, write 4 int, and 2 byte array
    Mockito.verify(mockNativeHandler, Mockito.times(1)).call(NativeReduceTask.RUN, null);
    Mockito.verify(mockWriter, Mockito.times(1)).collect(Matchers.any(InputBuffer.class));

    Mockito.verify(mockNativeHandler, Mockito.times(1)).close();
    Mockito.verify(mockWriter, Mockito.times(1)).close();
    Mockito.verify(mockLoader, Mockito.times(1)).close();
  }

  public void testLoad() throws IOException {

    this.handler = new NativeReduceTask(mockNativeHandler, null, mockLoader);

    Mockito.when(mockLoader.load()).thenReturn(100);
    final ReadWriteBuffer result = handler.onCall(NativeReduceTask.LOAD, null);
    Assert.assertEquals(100, result.readInt());
    Mockito.verify(mockLoader, Mockito.times(1)).load();
  }

  public void testUnRecognizedCommand() throws IOException {

    this.handler = new NativeReduceTask(mockNativeHandler, null, mockLoader);

    boolean ioExceptionThrown = false;
    try {
      handler.onCall(new Command(-1), null);
    } catch (final IOException e) {
      ioExceptionThrown = true;
    }
    Assert.assertEquals(true, ioExceptionThrown);
  }
}
