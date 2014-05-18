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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;
import org.mockito.Mockito;

@SuppressWarnings({ "deprecation" })
public class TestNativeMapTask extends TestCase {

  int bufferSize = 100; // bytes
  private NativeMapTask handler;
  private TaskAttemptID id;
  private INativeHandler nativeHandler;
  private String localDir = "build/test/mapred/local";

  @Override
  public void setUp() throws IOException {
    this.nativeHandler = Mockito.mock(INativeHandler.class);
    this.id = new TaskAttemptID();
    final JobConf jobConf = new JobConf();
    jobConf.set(OutputUtil.NATIVE_TASK_OUTPUT_MANAGER,
        "org.apache.hadoop.mapred.nativetask.util.LocalJobOutputFiles");
    jobConf.set("mapred.local.dir", localDir);
    this.handler = new NativeMapTask(jobConf, nativeHandler, id);
  }

  public void testOnCall() throws IOException {
    boolean thrown = false;
    try {
      handler.onCall(new Command(-1), null);
    } catch(final IOException e) {
      thrown = true;
    }
    Assert.assertTrue("exception thrown", thrown);

    final String expectedOutputPath = localDir + "/output/file.out";
    final String expectedOutputIndexPath = localDir + "/output/file.out.index";
    final String expectedSpillPath = localDir + "/output/spill0.out";

    final String outputPath = handler.onCall(NativeMapTask.GET_OUTPUT_PATH, null).readString();
    Assert.assertEquals(expectedOutputPath, outputPath);

    final String outputIndexPath = handler.onCall(NativeMapTask.GET_OUTPUT_INDEX_PATH, null).readString();
    Assert.assertEquals(expectedOutputIndexPath, outputIndexPath);

    final String spillPath = handler.onCall(NativeMapTask.GET_SPILL_PATH, null).readString();
    Assert.assertEquals(expectedSpillPath, spillPath);
  }

  public void testNativeMapTask() throws IOException {
    Mockito.when(nativeHandler.call(NativeMapTask.RUN, null)).thenReturn(null);

    handler.run();
    handler.close();
    handler.close();
    
    // flush once, write 4 int, and 2 byte array
    Mockito.verify(nativeHandler, Mockito.times(1)).call(NativeMapTask.RUN, null);
    Mockito.verify(nativeHandler, Mockito.times(1)).close();
  }
}
