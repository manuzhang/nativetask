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
package org.apache.hadoop.mapred.nativetask.mem;

import org.apache.hadoop.mapred.nativetask.tools.TeraSort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TerasortTest {
	private String terainput = "./testfile/terainput";
	private String teraoutput = "./testfile/output/teraNativeOut";
	@Before
	public void startUp(){
		
	}
	@After
	public void tearDown(){
		
	}
	@Test
	public void testTeraSort(){
		String[] args = {terainput,teraoutput};
		int exitCode = -1;
		try {
			exitCode = new TeraSort().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.err.println("testTeraSort finished with exitCode = "+exitCode);
		}
	}
}
