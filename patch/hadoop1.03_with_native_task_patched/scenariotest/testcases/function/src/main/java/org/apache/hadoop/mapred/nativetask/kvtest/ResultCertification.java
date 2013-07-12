package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultCertification {
	/**
	 * verify the result
	 * @param sample :nativetask output
	 * @param source :yuanwenjian
	 */
	public static String verify(String sample,String source){
		FSDataInputStream sourcein = null;
		FSDataInputStream samplein = null;
		
		String sampleline = null;
		String sourceline = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			sourcein = fs.open(new Path(source));
			samplein = fs.open(new Path(sample));
			
			if(samplein.available()!=sourcein.available()){
				return "0";
			}
			while(samplein.available()>0&&sourcein.available()>0){
				sampleline = samplein.readLine();
				sourceline = sourcein.readLine();
				if(sampleline.equals(sourceline))
					;
				else
					return "0";
			}
			return "1";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "0";
		}finally{
			try {
				samplein.close();
				sourcein.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
