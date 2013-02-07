package org.mpilone.hazelcastmq.stomp;

import java.io.*;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An output stream for STOMP frames. This implementation is thread-safe,
 * therefore multiple threads can write frames and they will be process fairly.
 * 
 * @author mpilone
 */
public class FrameOutputStream implements Closeable {

  /**
   * The null terminator that must appear after each STOMP frame.
   */
  private static final char NULL_CHARACTER = '\0';

  /**
   * The mutex to make frame writing thread safe.
   */
  private final Lock FRAME_WRITE_GUARD = new ReentrantLock(true);

  /**
   * The low level output stream from which to write.
   */
  private OutputStream outstream;

  /**
   * The log for this class.
   */
  // private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * Constructs the frame output stream which will read from the given output
   * stream.
   * 
   * @param outstream
   *          the low level output stream from which to write
   */
  public FrameOutputStream(OutputStream outstream) {
    this.outstream = outstream;
  }

  /**
   * Writes the given frame to the output stream. Defaults to terminating the
   * frame.
   * 
   * @param frame
   *          the frame to write
   * @throws IOException
   *           if there is an error on the underlying stream
   */
  public void write(Frame frame) throws IOException {
    write(frame, true);
  }

  /**
   * Writes the frame to the output stream. The termination character will only
   * be written if <code>terminate</code> is true. Setting
   * <code>termination</code> to false is useful if the frame is being printed
   * for debugging or in error messages.
   * 
   * @param frame
   *          the frame to write
   * @param terminate
   *          true to write the null termination character, false to not write
   *          it
   * @throws IOException
   *           if there is an error on the underlying stream
   */
  public void write(Frame frame, boolean terminate) throws IOException {
    FRAME_WRITE_GUARD.lock();
    try {
      unguardedWrite(frame, terminate);
    }
    finally {
      FRAME_WRITE_GUARD.unlock();
    }
  }

  /**
   * Writes the frame to the output stream. This method is NOT thread-safe
   * therefore the caller must provide any synchronization required.
   * 
   * @param frame
   *          the frame to write
   * @param terminate
   *          true to write the null termination character, false to not write
   *          it
   * @throws IOException
   *           if there is an error on the underlying stream
   */
  private void unguardedWrite(Frame frame, boolean terminate)
      throws IOException {
    PrintWriter writer = new PrintWriter(outstream);

    // Write the command
    writer.append(frame.getCommand().name()).append('\n');
    // log.debug("Wrote command: " + frame.getCommand());

    // Write the headers
    for (Map.Entry<String, String> header : frame.getHeaders().entrySet()) {
      String key = header.getKey();
      String value = header.getValue();

      // TODO encode header value
      writer.append(key).append(':').append(value).append('\n');
    }
    // log.debug("Wrote headers: " + frame.getHeaders());

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
    // log.debug("Wrote body: " + frame.getBody());

    if (terminate) {
      // Finally the terminator.
      outstream.write(NULL_CHARACTER);
    }
    outstream.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    outstream.close();
  }
}
