package org.mpilone.stomp.server;

import static org.mpilone.stomp.shared.StompConstants.UTF_8;

import org.mpilone.stomp.shared.*;

import io.netty.buffer.*;
import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
public class ErrorWritingHandler extends SimpleChannelInboundHandler<Frame> {

  public ErrorWritingHandler() {
    super(Frame.class, true);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {
    ctx.fireChannelRead(frame);
  }

  private StompClientException unwrapClientException(Throwable cause) {
    if (cause == null) {
      return null;
    }
    else if (cause instanceof StompClientException) {
      return (StompClientException) cause;
    }
    else {
      return unwrapClientException(cause.getCause());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws
      Exception {

    FrameBuilder fb = FrameBuilder.command(Command.ERROR).
        headerContentTypeText();
    StompClientException clientEx = unwrapClientException(cause);

    if (clientEx != null) {
      fb.header(Headers.MESSAGE, clientEx.getMessage());
      ByteBuf out = Unpooled.buffer();

      out.writeBytes("The original message:\n".getBytes(UTF_8));

      out.writeBytes("----------------\n".getBytes(UTF_8));
      Frame frame = clientEx.getFrame();
      if (frame != null) {
        StompFrameEncoder.encodeFrame(frame, out, false);
      }

      out.writeBytes("----------------\n".getBytes(UTF_8));
      String details = clientEx.getDetails();
      if (details != null) {
        out.writeBytes(details.getBytes(UTF_8));
      }

      fb.body(out.array());
      out.release();
    }
    else {
      fb.body("Internal server error.");

      // TODO: log this?
      cause.printStackTrace();
    }

    fb.headerContentLength();
    ctx.writeAndFlush(fb.build());
    ctx.close();
  }

}
