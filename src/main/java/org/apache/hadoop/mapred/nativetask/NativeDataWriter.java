package org.apache.hadoop.mapred.nativetask;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public abstract class NativeDataWriter extends OutputStream implements
    DataOutput {
  public abstract int reserve(int length) throws IOException;
}
