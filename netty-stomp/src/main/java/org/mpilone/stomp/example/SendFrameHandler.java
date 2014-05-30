
package org.mpilone.stomp.example;

import org.mpilone.stomp.shared.Frame;
import org.mpilone.stomp.shared.Headers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author mpilone
 */
public class SendFrameHandler extends SimpleChannelInboundHandler<Frame> {

  private final InMemoryBroker broker;

  public SendFrameHandler(InMemoryBroker broker) {
    super(Frame.class, true);

    this.broker = broker;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    switch (frame.getCommand()) {
      case SEND:
        String destination = frame.getHeaders().get(Headers.DESTINATION);
        String contentType = frame.getHeaders().get(Headers.CONTENT_TYPE);

        if (contentType == null) {
          contentType = "application/octet-stream";
        }

        // Create a message frame and send it via the broker.
        broker.publish(destination, contentType, frame.getBody());
        break;

      default:
        // no op
        break;
    }

    ctx.fireChannelRead(frame);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.println("SendFrameHandler: Channel inactive.");
    super.channelInactive(ctx);
  }

}
