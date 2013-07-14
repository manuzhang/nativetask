package org.apache.hadoop.mapred.nativetask.handlers;

import org.apache.hadoop.mapred.nativetask.INativeHandler;

interface ICombineHandler extends INativeHandler {
  public int combine();

  public long getId(); 
}
