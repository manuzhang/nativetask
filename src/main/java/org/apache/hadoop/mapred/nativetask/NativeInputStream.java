package org.apache.hadoop.mapred.nativetask;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NativeInputStream extends NativeDataReader {
  private ByteBuffer buffer;

  public NativeInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    return buffer.get();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    buffer.get(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    buffer.get(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    int remains = buffer.remaining();
    int skip = (remains < n) ? remains : n;
    int current = buffer.position();
    buffer.position(current + skip);
    return skip;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return (buffer.get() == 1) ? true : false;
  }

  @Override
  public byte readByte() throws IOException {
    return buffer.get();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    int ch = buffer.get();
    if (ch < 0)
      throw new EOFException();
    return ch;
  }

  @Override
  public short readShort() throws IOException {
    return buffer.getShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return buffer.getShort();
  }

  @Override
  public char readChar() throws IOException {
    return buffer.getChar();
  }

  @Override
  public int readInt() throws IOException {
    return buffer.getInt();
  }

  @Override
  public long readLong() throws IOException {
    return buffer.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    return buffer.getFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return buffer.getDouble();
  }

  @Override
  public String readLine() throws IOException {
    throw new IOException("NativeInputStream don't support readLine()");
  }

  @Override
  public String readUTF() throws IOException {
    throw new IOException("NativeInputStream don't support readUTF()");
  }
}
