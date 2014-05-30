package org.mpilone.hazelcastmq.stomp.server;


import org.mpilone.hazelcastmq.core.HazelcastMQMessage;
import org.mpilone.stomp.Command;
import org.mpilone.stomp.Frame;

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

  @Override
  public HazelcastMQMessage fromFrame(Frame frame) {

    HazelcastMQMessage msg = new HazelcastMQMessage();

    org.mpilone.stomp.Headers headers = frame.getHeaders();

    for (String name : headers.getHeaderNames()) {
      String value = headers.get(name);

      msg.getHeaders().put(name, value);
    }

    msg.setBody(frame.getBody());

    return msg;
  }

  @Override
  public Frame toFrame(HazelcastMQMessage msg) {
    Frame frame = new Frame(Command.MESSAGE);

    org.mpilone.stomp.Headers headers = frame.getHeaders();
    for (String name : msg.getHeaders().getHeaderNames()) {
      String value = msg.getHeaders().get(name);

      headers.put(name, value);
    }

    frame.setBody(msg.getBody());

    return frame;
  }
}
