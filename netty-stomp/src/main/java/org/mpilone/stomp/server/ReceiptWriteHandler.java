package org.mpilone.stomp.server;

import org.mpilone.stomp.shared.Frame;
import org.mpilone.stomp.shared.FrameBuilder;
import org.mpilone.stomp.shared.Headers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author mpilone
 */
public class ReceiptWriteHandler extends SimpleChannelInboundHandler<Frame> {

  public ReceiptWriteHandler() {
    super(Frame.class, true);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    String receiptId = frame.getHeaders().get(Headers.RECEIPT);
    if (receiptId != null) {
      Frame resp = FrameBuilder.receipt(receiptId).build();
      ctx.writeAndFlush(resp);
    }

    ctx.fireChannelRead(frame);
  }

}
