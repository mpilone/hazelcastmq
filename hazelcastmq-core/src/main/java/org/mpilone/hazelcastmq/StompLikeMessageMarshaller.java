package org.mpilone.hazelcastmq;

import java.io.*;
import java.util.Map;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A message marshaller that uses a STOMP (http://stomp.github.com/) like
 * message format. While not the most efficient format, its plain text nature
 * allows for easy monitoring, inspection, and debugging during transport.
 * 
 * @author mpilone
 */
public class StompLikeMessageMarshaller implements MessageMarshaller {

  /**
   * The NULL character to write at the end of STOMP message.
   */
  private static final char NULL_CHARACTER = (char) 0x00;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.prss.contentdepot.integration.util.camel.hazelcastjms.MessageMarshaller
   * #marshal(javax.jms.Message)
   */
  @Override
  public byte[] marshal(Message message) throws IOException, JMSException {

    ByteArrayOutputStream outstream = new ByteArrayOutputStream();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        outstream, "UTF-8"));

    // Write the header
    writer.append("MESSAGE\n");

    // Write the general headers
    HazelcastMQMessage hzMessage = (HazelcastMQMessage) message;
    marshalProperties(hzMessage.getHeaders(), writer);
    marshalProperties(hzMessage.getProperties(), writer);

    // Write the message specific portion. Currently only text messages are
    // supported.
    if (message instanceof HazelcastMQTextMessage) {
      // End the headers
      writer.append("\n");

      // Write the text
      writer.append(((HazelcastMQTextMessage) message).getText());
    }
    else {
      throw new UnsupportedOperationException(
          "Only TextMessages are currently supported.");
    }

    // Write the terminating NULL character.
    writer.append("\n");
    writer.append(NULL_CHARACTER);
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
  private void marshalProperties(Properties props, Writer writer)
      throws IOException {
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      writer.append(entry.getKey().toString()).append(":");
      writer.append(entry.getValue().toString().replace('\n', ' '));
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
  public Message unmarshal(byte[] msgData) throws IOException, JMSException {

    ByteArrayInputStream instream = new ByteArrayInputStream(msgData);
    BufferedReader reader = new BufferedReader(new InputStreamReader(instream,
        "UTF-8"));

    // The command. Must be MESSAGE.
    String line = reader.readLine();

    if (line == null || !line.equals("MESSAGE")) {
      throw new IOException("Invalid Hazelcast JMS message. The message must "
          + "start with MESSAGE command.");
    }

    Properties headers = new Properties();
    Properties properties = new Properties();

    // Read until we find a blank line.
    while ((line = reader.readLine()) != null) {

      if (line.isEmpty()) {
        // We reached the end of the headers.
        break;
      }

      int pos = line.indexOf(':');
      if (pos == -1) {
        // Log the invalid header and ignore it.
        continue;
      }

      String key = line.substring(0, pos);
      String value = pos < line.length() ? line.substring(pos + 1,
          line.length()) : "";

      if (key.startsWith("JMS") || key.startsWith("HZJMS")) {
        headers.setProperty(key, value);
      }
      else {
        properties.setProperty(key, value);
      }
    }

    // We only support text messages right now.
    HazelcastMQTextMessage message = new HazelcastMQTextMessage();
    message.getHeaders().putAll(headers);
    message.getProperties().putAll(properties);

    // Read until we find the null character and set the body.
    StringBuilder body = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      if (!line.isEmpty() && line.charAt(0) == NULL_CHARACTER) {
        break;
      }
      else if (body.length() > 0) {
        body.append("\n");
      }

      body.append(line);
    }

    message.setText(body.toString());

    return message;
  }
}
