package org.mpilone.hazelcastmq.stomp.server;

import java.util.Map;

import org.mpilone.hazelcastmq.core.HazelcastMQMessage;
import org.mpilone.hazelcastmq.stomp.Command;
import org.mpilone.hazelcastmq.stomp.Frame;

/**
 * Converts a STOMP Frame to and from a {@link HazelcastMQMessage}. This
 * implementation follows the basic rules defined by ActiveMQ's implementation
 * found at http://activemq.apache.org/stomp.html.
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

  // /*
  // * (non-Javadoc)
  // *
  // * @see
  // * org.mpilone.hazelcastmq.stomper.FrameConverter#fromFrameDestination(java
  // * .lang.String, javax.jms.Session)
  // */
  // @Override
  // public Destination fromFrameDestination(String destName)
  // {
  // if (destName.startsWith(QUEUE_PREFIX)) {
  // destName = destName.substring(QUEUE_PREFIX.length(), destName.length());
  // return session.createQueue(destName);
  // }
  // else if (destName.startsWith(TOPIC_PREFIX)) {
  // destName = destName.substring(TOPIC_PREFIX.length(), destName.length());
  // return session.createTopic(destName);
  // }
  // else {
  // throw new StompException("Destination prefix is not valid.");
  // }
  // }

  // /*
  // * (non-Javadoc)
  // *
  // * @see
  // * org.mpilone.hazelcastmq.stomper.FrameConverter#toFrameDestination(javax
  // * .jms.Destination)
  // */
  // @Override
  // public String toFrameDestination(Destination destination) {
  //
  // if (destination instanceof TemporaryQueue) {
  // return QUEUE_PREFIX + ((TemporaryQueue) destination).getQueueName();
  // }
  // else if (destination instanceof Queue) {
  // return QUEUE_PREFIX + ((Queue) destination).getQueueName();
  // }
  // else if (destination instanceof TemporaryTopic) {
  // return TOPIC_PREFIX + ((TemporaryTopic) destination).getTopicName();
  // }
  // else if (destination instanceof Topic) {
  // return TOPIC_PREFIX + ((Topic) destination).getTopicName();
  // }
  // else {
  // throw new StompException("Destination prefix cannot be determied.");
  // }
  // }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.stomper.FrameConverter#fromFrame(org.mpilone.
   * hazelcastmq.stomper.Frame, javax.jms.Session)
   */
  @Override
  public HazelcastMQMessage fromFrame(Frame frame) {

    HazelcastMQMessage msg = new HazelcastMQMessage();

    Map<String, String> headers = frame.getHeaders();

    for (Map.Entry<String, String> entry : headers.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();

      msg.getHeaders().put(name, value);
    }

    msg.setBody(frame.getBody());

    return msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.stomper.FrameConverter#toFrame(javax.jms.Message)
   */
  @Override
  public Frame toFrame(HazelcastMQMessage msg) {
    Frame frame = new Frame(Command.MESSAGE);

    Map<String, String> headers = frame.getHeaders();
    for (String name : msg.getHeaders().getHeaderNames()) {
      String value = msg.getHeaders().get(name);

      headers.put(name, value);
    }

    frame.setBody(msg.getBody());

    return frame;
  }
}
