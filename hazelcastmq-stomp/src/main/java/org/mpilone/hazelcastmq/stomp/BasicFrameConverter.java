package org.mpilone.hazelcastmq.stomp;


import org.mpilone.hazelcastmq.core.Message;
import org.mpilone.hazelcastmq.core.MessageBuilder;
import org.mpilone.yeti.*;

/**
 * Converts a STOMP Frame to and from a {@link Message}. This
 * implementation simply does a 1 to 1 copy of the headers and body from a frame
 * to a message.
  *
 * @author mpilone
 */
public class BasicFrameConverter implements FrameConverter {

  @Override
  public Message<?> fromFrame(Frame frame) {

    MessageBuilder builder = MessageBuilder.withPayload(frame.getBody());

    final org.mpilone.yeti.Headers headers = frame.getHeaders();
    headers.getHeaderNames().forEach((name) -> {
      builder.setHeader(name, headers.get(name));
    });

    return builder.build();
  }

  @Override
  public Frame toFrame(Message<?> msg) {

    FrameBuilder fb = FrameBuilder.command(Command.MESSAGE);

    // Body
    Object payload = msg.getPayload();
    if (payload instanceof byte[]) {
      fb.body((byte[]) payload);
      fb.headerContentTypeOctetStream();
    }
    else if (payload != null) {
      fb.body(payload.toString());
      fb.headerContentTypeText();
    }
    fb.headerContentLength();

    // Headers
    msg.getHeaders().entrySet().forEach(entry -> {
      String value = entry.getValue() == null ? null : entry.getValue().
          toString();
      fb.header(entry.getKey(), value);
    });

    return fb.build();
  }
}
