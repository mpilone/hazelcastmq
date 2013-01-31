package org.mpilone.hazelcastmq.stomper;

import static org.mpilone.hazelcastmq.stomper.StompConstants.NUL;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

class FrameInputStream implements Closeable {

  private InputStream instream;

  public FrameInputStream(InputStream instream) {
    this.instream = instream;
  }

  public Frame read() throws IOException {

    Frame frame = new Frame();

    readCommand(frame);
    readHeaders(frame);
    readBody(frame);

    return frame;
  }

  private void readBody(Frame frame) throws IOException {
    ByteArrayOutputStream bodyBuf = new ByteArrayOutputStream();

    // See if we have a content-length header.
    if (frame.getHeaders().containsKey("content-length")) {
      // Read the number of bytes specified in the content-length header.
      byte[] buf = new byte[1024];

      int bytesToRead = Integer.valueOf(frame.getHeaders()
          .get("content-length"));

      while (bytesToRead > 0) {
        int read = instream.read(buf, 0, Math.min(bytesToRead, buf.length));

        bodyBuf.write(buf, 0, read);
        bytesToRead -= read;
      }

      // Next byte should be a null character or something is wrong.
      int b = instream.read();
      if ((char) b != NUL) {
        // TODO
      }
    }
    else {
      // Read until we hit the null character.
      int b = instream.read();
      while (b != NUL) {
        bodyBuf.write(b);
        b = instream.read();
      }
    }

    frame.setBody(bodyBuf.toByteArray());
  }

  private void readHeaders(Frame frame) throws IOException {

    Map<String, String> headers = new HashMap<String, String>();

    String line = readLine();
    while (!line.isEmpty()) {
      int pos = line.indexOf(':');
      if (pos > 0) {
        String key = line.substring(0, pos);
        String value = line.substring(pos + 1, line.length());

        if (!headers.containsKey(key)) {

          // TODO decode the header
          headers.put(key, value);
        }
      }
    }

    frame.setHeaders(headers);
  }

  private void readCommand(Frame frame) throws IOException {

    String line;
    do {
      line = readLine();
    }
    while (!line.isEmpty());

    frame.setCommand(Command.valueOf(line));
  }

  private String readLine() throws IOException {

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    boolean eol = false;

    while (!eol) {
      int data = instream.read();
      if ((char) data == '\n') {
        eol = true;
      }
      else if (data == 0) {
        return null;
      }
      else {
        buf.write(data);
      }
    }

    return new String(buf.toByteArray(), "UTF-8");
  }

  @Override
  public void close() throws IOException {
    instream.close();
  }

}
