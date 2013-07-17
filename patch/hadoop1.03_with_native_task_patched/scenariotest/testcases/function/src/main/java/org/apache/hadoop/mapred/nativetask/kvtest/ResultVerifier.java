package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultVerifier {
	/**
	 * verify the result
	 * 
	 * @param sample
	 *            :nativetask output
	 * @param source
	 *            :yuanwenjian
	 */
	public static String verify(String sample, String source) {
		FSDataInputStream sourcein = null;
		FSDataInputStream samplein = null;

		String sampleline = null;
		String sourceline = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path sourcepath = new Path(source);
			Path samplepath = new Path(sample);
			if (fs.exists(sourcepath) && fs.exists(samplepath)) {
				sourcein = fs.open(sourcepath);
				samplein = fs.open(samplepath);
			} else
				return "0";
			if (samplein.available() != sourcein.available()) {
				return "0";
			}
			while (samplein.available() > 0 && sourcein.available() > 0) {
				sampleline = samplein.readLine();
				sourceline = sourcein.readLine();
				System.err.println(sampleline+"\t"+sourceline);
				if (sampleline.equals(sourceline))
					;
				else {
					return "0";
				}
			}
			System.err.println("----------------file matched-------------");
			return "1";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "0";
		} finally {
			try {
				if (samplein != null)
					samplein.close();
				if (sourcein != null)
					sourcein.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static void main(String[] args) {
	}
}
