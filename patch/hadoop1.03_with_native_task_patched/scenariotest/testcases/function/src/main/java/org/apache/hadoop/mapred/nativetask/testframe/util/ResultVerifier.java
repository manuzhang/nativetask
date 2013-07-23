package org.apache.hadoop.mapred.nativetask.testframe.util;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ResultVerifier {
	/**
	 * verify the result
	 * 
	 * @param sample
	 *            :nativetask output
	 * @param source
	 *            :yuanwenjian
	 * @throws Exception 
	 */
	public static boolean verify(String sample, String source) throws Exception {
		FSDataInputStream sourcein = null;
		FSDataInputStream samplein = null;

		String sampleline = null;
		String sourceline = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path hdfssource = new Path(source);
			Path[] sourcepaths = FileUtil.stat2Paths(fs.listStatus(hdfssource));
			// Path hdfssample = new Path(sample);
			// FileStatus[] samplepaths = fs.listStatus(hdfssample);

			for (int i = 0; i < sourcepaths.length; i++) {
				Path sourcepath = sourcepaths[i];
				// op result file start with "part-r" like part-r-00000
				if (!sourcepath.getName().startsWith("part-r"))
					continue;
				Path samplepath = new Path(sample + "/" + sourcepath.getName());
				// compare
				if (fs.exists(sourcepath) && fs.exists(samplepath)) {
					sourcein = fs.open(sourcepath);
					samplein = fs.open(samplepath);
				} else{
					System.err.println("result file not found:" + sourcepath
							+ " or " + samplepath);
					return false;
				}
				if (samplein.available() != sourcein.available()) {
					return false;
				}
				CRC32 sourcecrc,samplecrc ;
				samplecrc= new CRC32();
				sourcecrc = new CRC32();
				byte[] bufin = new byte[1<<16];
				while (samplein.available() > 0 && sourcein.available() > 0) {
					samplein.read(bufin);
					samplecrc.update(bufin);
					sourcein.read(bufin);
					sourcecrc.update(bufin);
					if (samplecrc.getValue()==sourcecrc.getValue())
						;
					else {
						return false;
					}
				}
			}

			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new Exception("verify exception :",e);
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
