package org.apache.hadoop.mapred.nativetask.disk;

import org.apache.hadoop.mapred.nativetask.tools.TeraSort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TerasortTest {
	private String terainput = "/terainput";
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
