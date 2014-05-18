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
package org.apache.hadoop.mapred.nativetask.cpu;

import org.apache.hadoop.mapred.nativetask.tools.Submitter;
import org.apache.hadoop.mapred.nativetask.tools.TeraSort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
	private String wordcountin = "./testfile/wordcountinput";
	private String wordcountout = "./testfile/output/wordcountOut";
	@Before
	public void startUp(){
		
	}
	@After
	public void tearDown(){
		
	}
	@Test
	public void testTeraSort(){
		String[] args = {"-reader","NativeTask.LineRecordReader ",
				"-writer","NativeTask.TextIntRecordWriter","-mapper",
				"NativeTask.WordCountMapper","-reducer","NativeTask.IntSumReducer",
				"-combiner","NativeTask.IntSumReducer","-input",wordcountin,
				"-output",wordcountout};
		int exitCode = -1;
		try {
			exitCode = new Submitter().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.err.println("testTeraSort finished with exitCode = "+exitCode);
		}
	}
}
