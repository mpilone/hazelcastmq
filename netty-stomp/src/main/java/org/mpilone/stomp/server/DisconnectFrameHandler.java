
package org.mpilone.stomp.server;

import org.mpilone.stomp.Frame;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author mpilone
 */
public class DisconnectFrameHandler extends SimpleChannelInboundHandler<Frame> {

  public DisconnectFrameHandler() {
    super(Frame.class, true);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    switch (frame.getCommand()) {
      case DISCONNECT:
        ctx.close();
        break;

      default:
        ctx.fireChannelRead(frame);
        break;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.println("DisconnectFrameHandler: Channel inactive.");
    super.channelInactive(ctx);
  }

}
