package org.mpilone.hazelcastmq.stomp.server;


import org.mpilone.hazelcastmq.core.HazelcastMQMessage;
import org.mpilone.yeti.Command;
import org.mpilone.yeti.Frame;
import org.mpilone.yeti.FrameBuilder;

/**
 * Converts a STOMP Frame to and from a {@link HazelcastMQMessage}. This
 * implementation simply does a 1 to 1 copy of the headers and body from a frame
 * to a message.
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

    org.mpilone.yeti.Headers headers = frame.getHeaders();

    for (String name : headers.getHeaderNames()) {
      String value = headers.get(name);

      msg.getHeaders().put(name, value);
    }

    msg.setBody(frame.getBody());

    return msg;
  }

  @Override
  public Frame toFrame(HazelcastMQMessage msg) {

    FrameBuilder fb = FrameBuilder.command(Command.MESSAGE);
    fb.body(msg.getBody());

    for (String name : msg.getHeaders().getHeaderNames()) {
      String value = msg.getHeaders().get(name);

      fb.header(name, value);
    }

    return fb.build();
  }
}
