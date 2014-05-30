package org.mpilone.stomp.server;

import org.mpilone.stomp.StompFrameEncoder;
import org.mpilone.stomp.Command;
import org.mpilone.stomp.Headers;
import org.mpilone.stomp.StompClientException;
import org.mpilone.stomp.FrameBuilder;
import org.mpilone.stomp.Frame;

import static org.mpilone.stomp.StompConstants.UTF_8;

import io.netty.buffer.*;
import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
public class ErrorWritingHandler extends ChannelHandlerAdapter {


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

}
