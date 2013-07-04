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
