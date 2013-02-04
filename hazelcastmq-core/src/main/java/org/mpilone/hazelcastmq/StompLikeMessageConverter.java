package org.mpilone.hazelcastmq;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.jms.*;

/**
 * A message converter that uses a STOMP (http://stomp.github.com/) like message
 * format (it is not guaranteed to be compatible with the STOMP specification).
 * While not the most efficient format, its plain text nature allows for easy
 * monitoring, inspection, and debugging during transport. An alternate, binary
 * implementation may be available in the future.
 * 
 * @author mpilone
 */
public class StompLikeMessageConverter implements MessageConverter {

  private static final String HEADER_CONTENT_LENGTH = "stomp-converter-content-length";
  /**
   * The NULL character to write at the end of STOMP message.
   */
  private static final char NULL_CHARACTER = '\0';

  /**
   * The UTF-8 character set used for all conversions.
   */
  private final static Charset UTF_8 = Charset.forName("UTF-8");

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.prss.contentdepot.integration.util.camel.hazelcastjms.MessageMarshaller
   * #marshal(javax.jms.Message)
   */
  @Override
  public byte[] fromMessage(Message message) throws IOException, JMSException {

    ByteArrayOutputStream outstream = new ByteArrayOutputStream();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        outstream, UTF_8));

    String command = "MESSAGE:";
    byte[] body = new byte[0];

    if (message instanceof HazelcastMQTextMessage) {
      TextMessage textMsg = (TextMessage) message;

      command += "TEXT";
      if (textMsg.getText() != null) {
        body = textMsg.getText().getBytes(UTF_8);
      }
    }
    else if (message instanceof HazelcastMQBytesMessage) {
      BytesMessage bytesMsg = (BytesMessage) message;

      command += "BYTES";
      body = new byte[(int) bytesMsg.getBodyLength()];
      bytesMsg.readBytes(body);
    }
    else {
      throw new UnsupportedOperationException(
          "Message type is not supported by this converter.");
    }

    // Write the header
    writer.append(command + "\n");

    // Write the general headers
    HazelcastMQMessage hzMessage = (HazelcastMQMessage) message;
    marshalProperties(hzMessage.getHeaders(), writer);
    marshalProperties(hzMessage.getProperties(), writer);
    writer.append(HEADER_CONTENT_LENGTH).append(":")
        .append(String.valueOf(body.length)).append("\n");

    // Blank line to start the body.
    writer.append("\n");
    writer.flush();

    outstream.write(body);

    // Write the terminating NULL character.
    outstream.write((byte) NULL_CHARACTER);
    // writer.append(NULL_CHARACTER);
    writer.close();

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
  private void marshalProperties(Map<String, String> props, Writer writer)
      throws IOException {
    for (Map.Entry<String, String> entry : props.entrySet()) {
      writer.append(entry.getKey()).append(":");
      writer.append(entry.getValue().replaceAll("\n", "\\n"));
      writer.append("\n");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.prss.contentdepot.integration.util.camel.hazelcastmq.ExchangeMarshaller
   * #unmarshal(java.io.InputStream, org.apache.camel.Exchange)
   */
  @Override
  public Message toMessage(byte[] msgData) throws IOException, JMSException {

    ByteArrayInputStream instream = new ByteArrayInputStream(msgData);

    // The command. Must be MESSAGE.
    String line = readLine(instream);

    if (line == null || !line.startsWith("MESSAGE:")) {
      throw new IOException("Invalid Hazelcast JMS message. The message must "
          + "start with MESSAGE command.");
    }

    long contentLength = 0;
    HazelcastMQMessage message;
    String command = line;
    Map<String, String> headers = new HashMap<String, String>();
    Map<String, String> properties = new HashMap<String, String>();

    // Read until we find a blank line (i.e. end of headers).
    boolean eoh = false;
    while (!eoh) {
      line = readLine(instream);

      // We reached the end of the headers.
      if (line.isEmpty()) {
        eoh = true;
        continue;
      }

      int pos = line.indexOf(':');
      if (pos == -1) {
        // Log the invalid header and ignore it.
        continue;
      }

      String key = line.substring(0, pos);
      String value = pos < line.length() ? line.substring(pos + 1,
          line.length()) : "";
      value = value.replaceAll("\\n", "\n");

      if (key.equals(HEADER_CONTENT_LENGTH)) {
        contentLength = Long.valueOf(value);
      }
      else if (key.startsWith("JMS") || key.startsWith("HZJMS")) {
        headers.put(key, value);
      }
      else {
        properties.put(key, value);
      }
    }

    // Read the body bytes.
    byte[] buf = new byte[1024];
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    int read = 0;
    while (contentLength > 0 && read != -1) {
      read = instream.read(buf, 0, Math.min((int) contentLength, buf.length));

      body.write(buf, 0, read);
      contentLength -= read;
    }
    body.close();

    // The last character should be the null character.
    int b = instream.read();
    if (b != NULL_CHARACTER) {
      throw new IOException("Invalid Hazelcast JMS message. The message must "
          + "end with NULL_CHARACTER.");
    }
    instream.close();

    // Create the JMS message and set the body appropriately.
    if (command.endsWith("TEXT")) {
      HazelcastMQTextMessage textMsg = new HazelcastMQTextMessage();
      textMsg.setText(new String(body.toByteArray(), UTF_8));
      message = textMsg;
    }
    else if (command.endsWith("BYTES")) {
      HazelcastMQBytesMessage bytesMsg = new HazelcastMQBytesMessage();
      bytesMsg.writeBytes(body.toByteArray());
      message = bytesMsg;
    }
    else {
      throw new IOException("Invalid Hazelcast JMS message. The message "
          + "type must be one of TEXT or BYTES.");
    }

    // Set the headers and properties.
    message.getProperties().putAll(properties);
    message.getHeaders().putAll(headers);

    return message;
  }

  /**
   * Reads a single line of input, returning the line as a UTF-8 string without
   * the newline character.
   * 
   * @param instream
   *          the input stream to read from
   * @return the line read without the newline character
   * @throws IOException
   */
  private String readLine(InputStream instream) throws IOException {
    int b;

    ByteArrayOutputStream outstream = new ByteArrayOutputStream();
    while ((b = instream.read()) != -1) {
      if ((char) b == '\n') {
        break;
      }
      else {
        outstream.write(b);
      }
    }
    outstream.close();

    return new String(outstream.toByteArray(), UTF_8);
  }
}
