package org.mpilone.hazelcastmq.stomper;

import static org.mpilone.hazelcastmq.stomp.IoUtil.UTF_8;

import java.util.Map;

import javax.jms.*;

import org.mpilone.hazelcastmq.stomp.Command;
import org.mpilone.hazelcastmq.stomp.Frame;
import org.mpilone.hazelcastmq.stomp.StompException;

/**
 * Converts a STOMP Frame to and from a JMS Message. This implementation follows
 * the basic rules defined by ActiveMQ's implementation found at
 * http://activemq.apache.org/stomp.html. However, this implementation uses the
 * "content-type" header to determine if a TextMessage or BytesMessage should be
 * used while ActiveMQ uses the content-length header.
 * 
 * @author mpilone
 */
public class DefaultFrameConverter implements FrameConverter {

  /**
   * The prefix to use for queue destinations defined in headers.
   */
  static final String QUEUE_PREFIX = "/queue/";

  /**
   * The prefix to use for topic destinations defined in headers.
   */
  static final String TOPIC_PREFIX = "/topic/";

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.stomper.FrameConverter#fromFrameDestination(java
   * .lang.String, javax.jms.Session)
   */
  @Override
  public Destination fromFrameDestination(String destName, Session session)
      throws JMSException {
    if (destName.startsWith(QUEUE_PREFIX)) {
      destName = destName.substring(QUEUE_PREFIX.length(), destName.length());
      return session.createQueue(destName);
    }
    else if (destName.startsWith(TOPIC_PREFIX)) {
      destName = destName.substring(TOPIC_PREFIX.length(), destName.length());
      return session.createTopic(destName);
    }
    else {
      throw new StompException("Destination prefix is not valid.");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.stomper.FrameConverter#toFrameDestination(javax
   * .jms.Destination)
   */
  @Override
  public String toFrameDestination(Destination destination) throws JMSException {

    if (destination instanceof TemporaryQueue) {
      return QUEUE_PREFIX + ((TemporaryQueue) destination).getQueueName();
    }
    else if (destination instanceof Queue) {
      return QUEUE_PREFIX + ((Queue) destination).getQueueName();
    }
    else if (destination instanceof TemporaryTopic) {
      return TOPIC_PREFIX + ((TemporaryTopic) destination).getTopicName();
    }
    else if (destination instanceof Topic) {
      return TOPIC_PREFIX + ((Topic) destination).getTopicName();
    }
    else {
      throw new StompException("Destination prefix cannot be determied.");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.stomper.FrameConverter#fromFrame(org.mpilone.
   * hazelcastmq.stomper.Frame, javax.jms.Session)
   */
  @Override
  public Message fromFrame(Frame frame, Session session) throws JMSException {

    Message msg = null;
    Map<String, String> headers = frame.getHeaders();

    String contentType = headers.get("content-type");
    if (contentType != null && contentType.startsWith("text/plain")) {
      msg = session.createTextMessage(new String(frame.getBody(), UTF_8));
    }
    else {
      BytesMessage bytesMsg = session.createBytesMessage();
      bytesMsg.writeBytes(frame.getBody());
      msg = bytesMsg;
    }

    // Convert the headers.
    String value = headers.get("correlation-id");
    if (value != null) {
      msg.setJMSCorrelationID(value);
    }

    value = headers.get("expires");
    if (value != null) {
      msg.setJMSExpiration(Long.valueOf(value));
    }

    value = headers.get("persistent");
    if (value != null) {
      msg.setJMSDeliveryMode(Integer.valueOf(value));
    }

    value = headers.get("priority");
    if (value != null) {
      msg.setJMSPriority(Integer.valueOf(value));
    }

    value = headers.get("reply-to");
    if (value != null) {
      msg.setJMSReplyTo(fromFrameDestination(value, session));
    }

    value = headers.get("type");
    if (value != null) {
      msg.setJMSType(value);
    }

    return msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.stomper.FrameConverter#toFrame(javax.jms.Message)
   */
  @Override
  public Frame toFrame(Message msg) throws JMSException {
    Frame frame = new Frame(Command.MESSAGE);
    Map<String, String> headers = frame.getHeaders();

    if (msg instanceof TextMessage) {
      headers.put("content-type", "text/plain");

      TextMessage textMsg = (TextMessage) msg;
      if (textMsg.getText() != null) {
        frame.setBody(textMsg.getText().getBytes(UTF_8));
      }
    }
    else if (msg instanceof BytesMessage) {
      headers.put("content-type", "application/octet-stream");

      BytesMessage bytesMsg = (BytesMessage) msg;
      byte[] data = new byte[(int) bytesMsg.getBodyLength()];
      bytesMsg.readBytes(data);
      frame.setBody(data);
    }

    // Convert the headers.
    String value = msg.getJMSCorrelationID();
    if (value != null) {
      headers.put("correlation-id", value);
    }

    value = String.valueOf(msg.getJMSExpiration());
    headers.put("expires", value);

    value = String.valueOf(msg.getJMSDeliveryMode());
    headers.put("persistent", value);

    value = String.valueOf(msg.getJMSPriority());
    headers.put("priority", value);

    Destination replyTo = msg.getJMSReplyTo();
    if (replyTo != null) {
      headers.put("reply-to", toFrameDestination(replyTo));
    }

    value = msg.getJMSMessageID();
    if (value != null) {
      headers.put("message-id", value);
    }

    value = msg.getJMSType();
    if (value != null) {
      headers.put("type", value);
    }

    return frame;
  }
}
