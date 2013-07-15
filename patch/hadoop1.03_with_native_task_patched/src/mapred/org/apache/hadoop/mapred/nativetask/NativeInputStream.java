package org.apache.hadoop.mapred.nativetask;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

public class NativeInputStream extends NativeDataReader {
  private ByteBuffer buffer;
  private char lineBuffer[];

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

    InputStream in = this;

    char buf[] = lineBuffer;

    if (buf == null) {
      buf = lineBuffer = new char[128];
    }

    int room = buf.length;
    int offset = 0;
    int c;

    loop: while (true) {
      switch (c = in.read()) {
      case -1:
      case '\n':
        break loop;

      case '\r':
        int c2 = in.read();
        if ((c2 != '\n') && (c2 != -1)) {
          if (!(in instanceof PushbackInputStream)) {
            in = new PushbackInputStream(in);
          }
          ((PushbackInputStream)in).unread(c2);
        }
        break loop;

      default:
        if (--room < 0) {
          buf = new char[offset + 128];
          room = buf.length - offset - 1;
          System.arraycopy(lineBuffer, 0, buf, 0, offset);
          lineBuffer = buf;
        }
        buf[offset++] = (char) c;
        break;
      }
    }
    if ((c == -1) && (offset == 0)) {
      return null;
    }
    return String.copyValueOf(buf, 0, offset);
  }

  @Override
  public final String readUTF() throws IOException {
      return readUTF(this);
  }

  public final static String readUTF(DataInput in) throws IOException {
    int utflen = in.readUnsignedShort();
    byte[] bytearr = null;
    char[] chararr = null;

    bytearr = new byte[utflen];
    chararr = new char[utflen];

    int c, char2, char3;
    int count = 0;
    int chararr_count = 0;

    in.readFully(bytearr, 0, utflen);

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      if (c > 127)
        break;
      count++;
      chararr[chararr_count++] = (char) c;
    }

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      switch (c >> 4) {
      case 0:
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
        /* 0xxxxxxx */
        count++;
        chararr[chararr_count++] = (char) c;
        break;
      case 12:
      case 13:
        /* 110x xxxx 10xx xxxx */
        count += 2;
        if (count > utflen)
          throw new UTFDataFormatException(
              "malformed input: partial character at end");
        char2 = (int) bytearr[count - 1];
        if ((char2 & 0xC0) != 0x80)
          throw new UTFDataFormatException("malformed input around byte "
              + count);
        chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
        break;
      case 14:
        /* 1110 xxxx 10xx xxxx 10xx xxxx */
        count += 3;
        if (count > utflen)
          throw new UTFDataFormatException(
              "malformed input: partial character at end");
        char2 = (int) bytearr[count - 2];
        char3 = (int) bytearr[count - 1];
        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
          throw new UTFDataFormatException("malformed input around byte "
              + (count - 1));
        chararr[chararr_count++] = (char) (((c & 0x0F) << 12)
            | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
        break;
      default:
        /* 10xx xxxx, 1111 xxxx */
        throw new UTFDataFormatException("malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararr_count);
  }
}

