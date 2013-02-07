package org.mpilone.hazelcastmq.stomp;

import static org.mpilone.hazelcastmq.stomp.IoUtil.UTF_8;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * An input stream for STOMP frames. Each request to {@link #read()} will block
 * until a complete frame is available.
 * 
 * @author mpilone
 */
public class FrameInputStream implements Closeable {

  /**
   * The null terminator that must appear after each STOMP frame.
   */
  public static final char NULL_CHARACTER = '\0';

  /**
   * The low level input stream from which to read.
   */
  private InputStream instream;

  /**
   * The log for this class.
   */
  // private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * Constructs the frame input stream which will read from the given input
   * stream.
   * 
   * @param instream
   *          the low level input stream from which to read
   */
  public FrameInputStream(InputStream instream) {
    this.instream = instream;
  }

  /**
   * Reads a complete STOMP frame and returns it. This method will block until a
   * complete frame is available or there is an error reading.
   * 
   * @return the frame read
   * @throws IOException
   *           if there is an error reading from the underlying stream
   */
  public Frame read() throws IOException {

    Frame frame = new Frame();

    readCommand(frame);
    // log.debug("Read command: " + frame.getCommand());

    readHeaders(frame);
    // log.debug("Read headers: " + frame.getHeaders());

    readBody(frame);
    // log.debug("Read body: " + frame.getBody());

    return frame;
  }

  /**
   * Reads the body of the frame using the content-length header if available.
   * The body will be set in the frame after reading.
   * 
   * @param frame
   *          the partial frame that has been read to this point
   * @throws IOException
   *           if there is an error reading from the underlying stream
   */
  private void readBody(Frame frame) throws IOException {
    ByteArrayOutputStream bodyBuf = new ByteArrayOutputStream();

    // See if we have a content-length header.
    if (frame.getHeaders().containsKey("content-length")) {
      // Read the number of bytes specified in the content-length header.
      byte[] buf = new byte[1024];

      int bytesToRead = Integer.valueOf(frame.getHeaders()
          .get("content-length"));

      int read = 0;
      while (bytesToRead > 0 && read != -1) {
        read = instream.read(buf, 0, Math.min(bytesToRead, buf.length));

        bodyBuf.write(buf, 0, read);
        bytesToRead -= read;
      }

      // Next byte should be a null character or something is wrong.
      int b = instream.read();
      if (b != NULL_CHARACTER) {
        throw new StompException("Stomp frame must end with a NULL terminator.");
      }
    }
    else {
      // Read until we hit the null character.
      int b = instream.read();
      while (b != NULL_CHARACTER) {
        bodyBuf.write(b);
        b = instream.read();
      }
    }

    frame.setBody(bodyBuf.toByteArray());
  }

  /**
   * Reads the headers of the frame if available. The headers will be set in the
   * frame after reading.
   * 
   * @param frame
   *          the partial frame that has been read to this point
   * @throws IOException
   *           if there is an error reading from the underlying stream
   */
  private void readHeaders(Frame frame) throws IOException {

    Map<String, String> headers = new HashMap<String, String>();

    // Read until we find a blank line (i.e. end of headers).
    boolean eoh = false;
    while (!eoh) {
      String line = readLine();

      if (line.isEmpty()) {
        eoh = true;
        continue;
      }

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

  /**
   * Reads the command of the frame. The command will be set in the frame after
   * reading.
   * 
   * @param frame
   *          the partial frame that has been read to this point
   * @throws IOException
   *           if there is an error reading from the underlying stream
   */
  private void readCommand(Frame frame) throws IOException {

    String line;
    do {
      line = readLine();
    }
    while (line.isEmpty());

    frame.setCommand(Command.valueOf(line));
  }

  /**
   * Reads a single line of UTF-8 text from the stream and returns once a new
   * line character is found.
   * 
   * @return the line of text read
   * @throws IOException
   *           if there is an error reading from the underlying stream
   */
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

    return new String(buf.toByteArray(), UTF_8);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    instream.close();
  }

}
