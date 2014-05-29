
package org.mpilone.stomp.server;

import org.mpilone.stomp.shared.Frame;
import org.mpilone.stomp.shared.FrameBuilder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author mpilone
 */
public class ConnectFrameHandler extends SimpleChannelInboundHandler<Frame> {

  private boolean connected = false;

  public ConnectFrameHandler() {
    super(Frame.class, true);
  }
  
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    Frame resp;

    switch (frame.getCommand()) {
      case STOMP:
      case CONNECT:
        if (checkConnected(ctx, false)) {
          // TODO: perform content negotiation, authentication,
          // heart-beat setup, etc.
          connected = true;
          resp = FrameBuilder.connected("1.2").build();
          ctx.writeAndFlush(resp);
          ctx.fireChannelRead(frame);
        }
        break;

      default:
        if (checkConnected(ctx, true)) {
          ctx.fireChannelRead(frame);
        }
        break;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.println("ConnectFrameHandler: Channel inactive.");

    connected = false;
    super.channelInactive(ctx);
  }

  private boolean checkConnected(ChannelHandlerContext ctx,
      boolean expectConnected) {
    boolean result = true;
    if (expectConnected && !connected) {
      result = false;

      Frame resp = FrameBuilder.error("Client already connected.").build();
      ctx.writeAndFlush(resp);
      ctx.close();
    }
    else if (!expectConnected && connected) {
      result = false;

      Frame resp = FrameBuilder.error("Client must be connected.").build();
      ctx.writeAndFlush(resp);
      ctx.close();
    }

    return result;
  }

}
