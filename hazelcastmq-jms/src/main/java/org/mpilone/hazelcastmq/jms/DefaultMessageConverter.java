package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import java.nio.charset.Charset;
import java.util.Map;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQMessage;
import org.mpilone.hazelcastmq.core.Headers;

/**
 * A message converter that uses a STOMP (http://stomp.github.com/) like message
 * format (it is not guaranteed to be compatible with the STOMP specification).
 * While not the most efficient format, its plain text nature allows for easy
 * monitoring, inspection, and debugging during transport. An alternate, binary
 * implementation may be available in the future.
 * 
 * @author mpilone
 */
public class DefaultMessageConverter implements MessageConverter {

  private static final String CONTENT_TYPE_TEXT = "text/plain";

  private static final String CONTENT_TYPE_OCTET = "application/octet-stream";

  private static final String MQ_HEADER_JMS_HEADER_PREFIX = "jms-header-";

  private static final String MQ_HEADER_JMS_PROPERTY_PREFIX = "jms-property-";

  /**
   * The UTF-8 character set used for all conversions.
   */
  private final static Charset UTF_8 = Charset.forName("UTF-8");

  @Override
  public HazelcastMQMessage fromJmsMessage(Message message) throws JMSException {

    HazelcastMQJmsMessage jmsMsg = (HazelcastMQJmsMessage) message;
    HazelcastMQMessage mqMsg = new HazelcastMQMessage();

    for (Map.Entry<String, String> entry : jmsMsg.getHeaders().entrySet()) {
      mqMsg.getHeaders().put(MQ_HEADER_JMS_HEADER_PREFIX + entry.getKey(),
          entry.getValue());
    }

    for (Map.Entry<String, String> entry : jmsMsg.getProperties().entrySet()) {
      mqMsg.getHeaders().put(MQ_HEADER_JMS_PROPERTY_PREFIX + entry.getKey(),
          entry.getValue());
    }

    if (jmsMsg.getJMSReplyTo() != null) {
      HazelcastMQJmsDestination replyToDest = (HazelcastMQJmsDestination) jmsMsg
          .getJMSReplyTo();
      mqMsg.getHeaders().put(Headers.REPLY_TO, replyToDest.getMqName());
    }

    if (jmsMsg.getJMSCorrelationID() != null) {
      mqMsg.getHeaders().put(Headers.CORRELATION_ID,
          jmsMsg.getJMSCorrelationID());
    }

    byte[] body = new byte[0];

    if (message instanceof HazelcastMQJmsTextMessage) {
      TextMessage textMsg = (TextMessage) message;
      if (textMsg.getText() != null) {
        body = textMsg.getText().getBytes(UTF_8);
      }
      mqMsg.setContentType(CONTENT_TYPE_TEXT);
    }
    else if (message instanceof HazelcastMQJmsBytesMessage) {
      BytesMessage bytesMsg = (BytesMessage) message;

      // Put Bytes message in read mode.
      bytesMsg.reset();

      body = new byte[(int) bytesMsg.getBodyLength()];
      bytesMsg.readBytes(body);
      mqMsg.setContentType(CONTENT_TYPE_OCTET);
    }
    else {
      throw new UnsupportedOperationException(
          format("Message type [%s] is not supported by this converter.",
              message.getClass().getName()));
    }

    mqMsg.setBody(body);
    mqMsg.getHeaders().put(Headers.CONTENT_LENGTH, String.valueOf(body.length));

    return mqMsg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.prss.contentdepot.integration.util.camel.hazelcastmq.ExchangeMarshaller
   * #unmarshal(java.io.InputStream, org.apache.camel.Exchange)
   */
  @Override
  public Message toJmsMessage(HazelcastMQMessage mqMsg) throws JMSException {

    HazelcastMQJmsMessage jmsMsg = null;

    String contentType = mqMsg.getContentType();
    byte[] body = mqMsg.getBody();

    if (contentType != null && contentType.equals(CONTENT_TYPE_TEXT)) {
      HazelcastMQJmsTextMessage textMsg = new HazelcastMQJmsTextMessage();

      if (body != null) {
        textMsg.setText(new String(mqMsg.getBody(), UTF_8));
      }

      jmsMsg = textMsg;
    }
    else {
      HazelcastMQJmsBytesMessage bytesMsg = new HazelcastMQJmsBytesMessage();

      if (body != null) {
        bytesMsg.writeBytes(body);
      }
      bytesMsg.reset();

      jmsMsg = bytesMsg;
    }

    // Copy all the headers
    for (String headerName : mqMsg.getHeaders().getHeaderNames()) {
      String headerValue = mqMsg.getHeaders().get(headerName);

      if (headerName.startsWith(MQ_HEADER_JMS_HEADER_PREFIX)) {
        jmsMsg.getHeaders().put(
            headerName.substring(MQ_HEADER_JMS_HEADER_PREFIX.length()),
            headerValue);
      }

      else if (headerName.startsWith(MQ_HEADER_JMS_PROPERTY_PREFIX)) {
        jmsMsg.getProperties().put(
            headerName.substring(MQ_HEADER_JMS_PROPERTY_PREFIX.length()),
            headerValue);
      }

      else if (headerName.equals(Headers.DESTINATION)) {
        jmsMsg.setJMSDestination(toJmsDestination(headerValue));
      }

      else if (headerName.equals(Headers.REPLY_TO)) {
        jmsMsg.setJMSReplyTo(toJmsDestination(headerValue));
      }

      else if (headerName.equals(Headers.CORRELATION_ID)) {
        jmsMsg.setJMSCorrelationID(headerValue);
      }

      else if (headerName.equals(Headers.EXPIRATION)) {
        jmsMsg.setJMSExpiration(Long.parseLong(headerValue));
      }

      else if (headerName.equals(Headers.MESSAGE_ID)) {
        jmsMsg.setJMSMessageID("ID:" + headerValue);
      }
    }

    return jmsMsg;
  }

  private Destination toJmsDestination(String mqDestination) {
    if (mqDestination.startsWith(Headers.DESTINATION_QUEUE_PREFIX)) {
      return new HazelcastMQJmsQueue(
          mqDestination.substring(Headers.DESTINATION_QUEUE_PREFIX.length()));
    }
    else if (mqDestination
        .startsWith(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX)) {
      return new HazelcastMQJmsTemporaryQueue(
          mqDestination.substring(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX
              .length()));
    }
    else if (mqDestination
        .startsWith(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX)) {
      return new HazelcastMQJmsTemporaryTopic(
          mqDestination.substring(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX
              .length()));
    }
    else if (mqDestination.startsWith(Headers.DESTINATION_TOPIC_PREFIX)) {
      return new HazelcastMQJmsTopic(
          mqDestination.substring(Headers.DESTINATION_TOPIC_PREFIX.length()));
    }
    else {
      throw new IllegalArgumentException(format(
          "Unsupported MQ destination type [%s].", mqDestination));
    }
  }
}
