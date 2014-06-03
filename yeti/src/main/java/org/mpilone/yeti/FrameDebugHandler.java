
package org.mpilone.yeti;

import java.util.Date;
import java.util.UUID;

import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
public class FrameDebugHandler extends SimpleChannelInboundHandler<Frame> {

  private final String instanceId;

  public FrameDebugHandler() {
    this(null);
  }

  public FrameDebugHandler(String instanceId) {
    super(Frame.class, true);

    this.instanceId = instanceId == null ? UUID.randomUUID().toString() :
        instanceId;
  }
  
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    System.out.printf("[%s] [%s] Received frame: %s\n", new Date(), instanceId,
        frame);
    ctx.fireChannelRead(frame);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.printf("[%s] [%s] Channel inactive.\n", new Date(), instanceId);
    super.channelInactive(ctx);
  }

}
