package org.mpilone.hazelcastmq.stomper;

import java.io.*;
import java.util.Map;

class FrameOutputStream implements Closeable {
  private static final int NUL = 0x00;

  private OutputStream outstream;

  public FrameOutputStream(OutputStream outstream) {
    this.outstream = outstream;
  }

  public void write(Frame frame) throws IOException {
    write(frame, true);
  }

  public void write(Frame frame, boolean terminate) throws IOException {

    PrintWriter writer = new PrintWriter(outstream);

    // Write the command
    writer.append(frame.getCommand().name()).append('\n');

    // Write the headers
    for (Map.Entry<String, String> header : frame.getHeaders().entrySet()) {
      String key = header.getKey();
      String value = header.getValue();

      // TODO encode header value
      writer.append(key).append(':').append(value).append('\n');
    }

    // If we have a body and we don't have a content-length header, write one.
    if (frame.getBody() != null
        && !frame.getHeaders().containsKey("content-length")) {
      writer.append("content-length:")
          .append(String.valueOf(frame.getBody().length)).append("\n");
    }

    // Blank line to separate headers from the body.
    writer.append('\n');
    writer.flush();

    // Write the body.
    if (frame.getBody() != null) {
      outstream.write(frame.getBody());
    }

    if (terminate) {
      // Finally the terminator.
      outstream.write(NUL);
    }
  }

  @Override
  public void close() throws IOException {
    outstream.close();
  }
}
