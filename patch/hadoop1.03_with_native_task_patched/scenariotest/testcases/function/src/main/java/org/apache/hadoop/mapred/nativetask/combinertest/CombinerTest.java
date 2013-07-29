package org.apache.hadoop.mapred.nativetask.combinertest;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.mapred.nativetask.testframe.util.ResultVerifier;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class CombinerTest {
	@Test
	public void testWordCountCombiner() {
		try {
			Job nativejob = CombinerJobFactory
					.getWordCountNativeJob("nativewordcount");
			Job normaljob = CombinerJobFactory
					.getWordCountNormalJob("normalwordcount");
			nativejob.waitForCompletion(true);
			normaljob.waitForCompletion(true);
			assertEquals(true, ResultVerifier.verify(
					nativejob.getConfiguration().get("fileoutputpath")+"/part-r-00000", 
					normaljob.getConfiguration().get("fileoutputpath")+"/part-r-00000")
					);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
