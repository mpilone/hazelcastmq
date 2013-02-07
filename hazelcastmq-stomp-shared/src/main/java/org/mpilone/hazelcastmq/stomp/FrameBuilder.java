package org.mpilone.hazelcastmq.stomp;

import static org.mpilone.hazelcastmq.stomp.IoUtil.UTF_8;

public class FrameBuilder {
  private Frame frame;

  private FrameBuilder() {
    frame = new Frame();
  }

  public static FrameBuilder unsubscribe(String id) {
    FrameBuilder fb = command(Command.UNSUBSCRIBE).header("id", id).header(
        "ack", "auto");
    return fb;
  }

  public static FrameBuilder send(String destination, String body) {
    FrameBuilder fb = command(Command.SEND).header("destination", destination)
        .header("content-type", "text/plain").body(body);
    return fb;
  }

  public static FrameBuilder subscribe(String destination, String id) {
    FrameBuilder fb = command(Command.SUBSCRIBE)
        .header("destination", destination).header("id", id)
        .header("ack", "auto");
    return fb;
  }

  public static FrameBuilder command(Command command) {
    FrameBuilder fb = new FrameBuilder();
    fb.frame.setCommand(command);
    return fb;
  }

  public FrameBuilder header(String name, String value) {
    frame.getHeaders().put(name, value);
    return this;
  }

  public FrameBuilder body(String body) {
    return body(body.getBytes(UTF_8));
  }

  public FrameBuilder body(byte[] body) {
    frame.setBody(body);
    return this;
  }

  public Frame build() {
    return frame;
  }

}
