package org.apache.hadoop.mapred.nativetask;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public abstract class NativeOutputStream extends NativeDataWriter {
  private ByteBuffer buffer;

  public NativeOutputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public synchronized void write(int v) throws IOException {
    buffer.put((byte) v);
  }

  @Override
  public int reserve(int length) throws IOException {
    if (buffer.remaining() < length) {
      flush();
    }
    if (buffer.remaining() < length) {
      throw new BufferOverflowException();
    }
    return buffer.remaining();
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    buffer.put(b, off, len);
  }

  @Override
  public abstract void flush() throws IOException;

  @Override
  abstract public void close() throws IOException;

  private final static byte TRUE = (byte) 1;
  private final static byte FALSE = (byte) 0;

  @Override
  public final void writeBoolean(boolean v) throws IOException {
    buffer.put(v ? TRUE : FALSE);
  }

  @Override
  public final void writeByte(int v) throws IOException {
    buffer.put((byte) v);
  }

  @Override
  public final void writeShort(int v) throws IOException {
    buffer.putShort((short) v);
  }

  @Override
  public final void writeChar(int v) throws IOException {
    buffer.put((byte) ((v >>> 8) & 0xFF));
    buffer.put((byte) ((v >>> 0) & 0xFF));
  }

  @Override
  public final void writeInt(int v) throws IOException {
    buffer.putInt(v);
  }

  @Override
  public final void writeLong(long v) throws IOException {
    buffer.putLong(v);
  }

  @Override
  public final void writeFloat(float v) throws IOException {
    buffer.putFloat(v);
  }

  @Override
  public final void writeDouble(double v) throws IOException {
    buffer.putDouble(v);
  }

  @Override
  public final void writeBytes(String s) throws IOException {
    int len = s.length();
    for (int i = 0; i < len; i++) {
      buffer.put((byte) s.charAt(i));
    }
  }

  @Override
  public final void writeChars(String s) throws IOException {
    int len = s.length();
    for (int i = 0; i < len; i++) {
      buffer.putChar(s.charAt(i));
    }
  }

  @Override
  public final void writeUTF(String str) throws IOException {
    writeUTF(str, this);
  }

  private static int writeUTF(String str, DataOutput out) throws IOException {
    int strlen = str.length();
    int utflen = 0;
    int c, count = 0;

    /* use charAt instead of copying String to char array */
    for (int i = 0; i < strlen; i++) {
      c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      } else if (c > 0x07FF) {
        utflen += 3;
      } else {
        utflen += 2;
      }
    }

    if (utflen > 65535)
      throw new UTFDataFormatException(
          "encoded string too long: " + utflen + " bytes");

    byte[] bytearr = new byte[utflen+2];
    bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
    bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);  

    int i=0;
    for (i=0; i<strlen; i++) {
      c = str.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) break;
      bytearr[count++] = (byte) c;
    }

    for (;i < strlen; i++){
      c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        bytearr[count++] = (byte) c;

      } else if (c > 0x07FF) {
        bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
        bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
      } else {
        bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
        bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
      }
    }
    out.write(bytearr, 0, utflen+2);
    return utflen + 2;
  }
}
