
package org.mpilone.stomp.client;

import org.mpilone.stomp.shared.Frame;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author mpilone
 */
public class ConnectedFrameHandler extends SimpleChannelInboundHandler<Frame> {

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {
    // TODO: Implement method
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
