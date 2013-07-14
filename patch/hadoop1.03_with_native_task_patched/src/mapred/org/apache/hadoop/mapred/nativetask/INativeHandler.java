package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * A Handler accept input, and give output
 * 
 */
public interface INativeHandler {

  /**
   * init the native handler
   */
  public void init(Configuration conf) throws IOException;

  /**
   * close the native handler
   */
  public void close() throws IOException;

  /**
   * called by java side, to get input buffer
   * 
   * @return
   */
  public NativeDataWriter getWriter();

  /**
   * called by java side, to get output buffer.
   * 
   * @return
   */
  public NativeDataReader getReader();

}
