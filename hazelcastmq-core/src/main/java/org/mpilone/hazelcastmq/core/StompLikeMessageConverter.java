package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;
import static org.mpilone.hazelcastmq.core.HazelcastMQConstants.UTF_8;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * A message converter that uses a STOMP (http://stomp.github.com/) like message
 * format (it is not guaranteed to be compatible with the STOMP specification).
 * While not the most efficient format, its plain text nature allows for easy
 * monitoring, inspection, and debugging during transport. The alternate,
 * {@link NoOpMessageConverter} delegates to Hazelcast's own serialization which
 * will be much faster.
  * 
 * @author mpilone
 */
public class StompLikeMessageConverter implements MessageConverter {

  private static final String MESSAGE_COMMAND = "HAZELCASTMQ-MESSAGE";

  private static final String HEADER_CONTENT_LENGTH =
      "stomp-converter-content-length";

  /**
   * The NULL character to write at the end of STOMP message.
   */
  private static final char NULL_CHARACTER = '\0';

  private static final char NEWLINE = '\n';

  @Override
  public Object fromMessage(HazelcastMQMessage message)
      throws HazelcastMQException {
    try {
      return doFromMessage(message);
    }
    catch (IOException ex) {
      throw new HazelcastMQException(
          "IO error converting message to message bytes.", ex);
    }
  }

  private byte[] doFromMessage(HazelcastMQMessage message) throws IOException {

    ByteArrayOutputStream outstream = new ByteArrayOutputStream();
    // BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
    // outstream, UTF_8));

    // Command
    writeLine(outstream, MESSAGE_COMMAND);

    // Write the general headers
    HazelcastMQMessage hzMessage = message;
    marshalHeaders(hzMessage.getHeaders(), outstream);

    byte[] body = message.getBody();
    if (body != null) {
      write(outstream, HEADER_CONTENT_LENGTH);
      outstream.write(':');
      writeLine(outstream, String.valueOf(body.length));

      // Blank line to start the body.
      outstream.write(NEWLINE);
      outstream.write(body);
    }
    else {
      outstream.write(NEWLINE);
    }

    // Write the terminating NULL character.
    outstream.write((byte) NULL_CHARACTER);

    return outstream.toByteArray();
  }

  /**
   * Marshals the given properties as STOMP message headers.
   * 
   * @param props
   *          the properties to marshal
   * @param writer
   *          the writer to write to
   * @throws IOException
   */
  private void marshalHeaders(Headers headers, OutputStream outstream)
      throws IOException {
    for (String name : headers.getHeaderNames()) {
      write(outstream, name);
      outstream.write(':');

      write(outstream, headers.get(name).replaceAll("\n", "\\n"));
      outstream.write('\n');
    }
  }

  private void write(OutputStream outstream, String value) throws IOException {
    outstream.write(value.getBytes(UTF_8));
  }

  private void writeLine(OutputStream outstream, String value)
      throws IOException {
    write(outstream, value);
    outstream.write(NEWLINE);
  }

  @Override
  public HazelcastMQMessage toMessage(Object msgData)
      throws HazelcastMQException {

    try {
      return doToMessage((byte[]) msgData);
    }
    catch (IOException ex) {
      throw new HazelcastMQException(
          "IO error converting message bytes to a message.", ex);
    }
  }

  private HazelcastMQMessage doToMessage(byte[] msgData) throws IOException {

    ByteBuffer byteBuf = ByteBuffer.wrap(msgData);

    // The command. Must be MESSAGE_COMMAND.
    String line = readLine(byteBuf);

    if (line == null || !line.equals(MESSAGE_COMMAND)) {
      throw new HazelcastMQException(format(
          "Invalid HazelcastMQ message. The message must "
              + "start with %s command.", MESSAGE_COMMAND));
    }

    int contentLength = 0;
    HazelcastMQMessage message = new HazelcastMQMessage();

    // Read until we find a blank line (i.e. end of headers).
    boolean eoh = false;
    while (!eoh) {
      line = readLine(byteBuf);

      // We reached the end of the headers.
      if (line.isEmpty()) {
        eoh = true;
        continue;
      }

      String[] header = line.split(":");
      String key = header[0];
      String value = header.length >= 2 ? header[1] : "";
      value = value.replaceAll("\\n", "\n");

      if (key.equals(HEADER_CONTENT_LENGTH)) {
        contentLength = Integer.parseInt(value);
      }
      else {
        message.getHeaders().put(key, value);
      }
    }

    // Read the body bytes.
    byte[] body = new byte[contentLength];
    byteBuf.get(body);

    // The last character should be the null character.
    int b = byteBuf.get();
    if (b != NULL_CHARACTER) {
      throw new HazelcastMQException(
          "Invalid Hazelcast JMS message. The message must "
              + "end with NULL_CHARACTER.");
    }

    message.setBody(body);

    return message;
  }

  private String readLine(ByteBuffer byteBuf) {

    byteBuf.mark();

    int length = 0;
    boolean found = false;
    while (!found && byteBuf.hasRemaining()) {
      found = (byteBuf.get() == NEWLINE);
      length++;
    }

    String line;
    if (found) {
      byteBuf.reset();
      byteBuf.limit(byteBuf.position() + length - 1);
      line = UTF_8.decode(byteBuf.slice()).toString();

      // Restore the limit and skip past the newline character.
      byteBuf.limit(byteBuf.capacity());
      byteBuf.position(byteBuf.position() + length);
    }
    else {
      line = "";
    }

    return line;
  }
}
