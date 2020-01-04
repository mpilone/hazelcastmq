
package org.mpilone.yeti;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.*;

/**
 * An inbound channel handler that logs all incoming and outgoing frames at the
 * DEBUG level. Inbound and outbound frame logging can be individually disabled
 * to reduce log output. If the DEBUG level of logging is not enabled, this
 * handler does nothing.
 *
 * @author mpilone
 */
public class FrameDebugHandler extends ChannelDuplexHandler {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(FrameDebugHandler.class);

  private boolean debugInbound;
  private boolean debugOutbound;

  /**
   * Constructs the handler with an automatically generated instance ID and both
   * inbound and outbound debugging enabled.
   */
  public FrameDebugHandler() {
    this(true, true);
  }

  /**
   * Constructs the handler with an automatically generated instance ID.
   *
   * @param debugInbound true to enable inbound debugging/logging
   * @param debugOutbound true to enable outbound debugging/logging
   */
  public FrameDebugHandler(boolean debugInbound, boolean debugOutbound) {
    this.debugInbound = debugInbound;
    this.debugOutbound = debugOutbound;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws
      Exception {

    if (log.isDebugEnabled() && debugInbound && msg instanceof Frame) {
      log.debug("Frame read from [{}]: {}", ctx.channel().
          remoteAddress(), msg);
    }

    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {

    if (log.isDebugEnabled() && debugOutbound && msg instanceof Frame) {
      log.debug("Frame write to [{}]: {}",
          ctx.channel().remoteAddress(), msg);
    }

    super.write(ctx, msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("Channel inactive for [{}].", ctx.channel().remoteAddress());
    }

    super.channelInactive(ctx);
  }
}
