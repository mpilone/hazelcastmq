
package org.mpilone.yeti;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import io.netty.channel.*;

/**
 * An inbound channel handler that prints frames to stdout for debugging
 * purposes. The handler shouldn't be used in a production environment.
 *
 * @author mpilone
 */
public class FrameDebugHandler extends ChannelDuplexHandler {

  private final String instanceId;
  private boolean debugInbound;
  private boolean debugOutbound;
  private final SimpleDateFormat formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * Constructs the handler with an automatically generated instance ID and only
   * inbound debugging enabled.
   */
  public FrameDebugHandler() {
    this(true, false);
  }

  /**
   * Constructs the handler with an automatically generated instance ID.
   *
   * @param debugInbound true to enable inbound debugging/logging
   * @param debugOutbound true to enable outbound debugging/logging
   */
  public FrameDebugHandler(boolean debugInbound, boolean debugOutbound) {
    this(debugInbound, debugOutbound, null);
  }

  /**
   * Constructs the handler with the specified instance ID.
   *
   * @param debugInbound true to enable inbound debugging/logging
   * @param debugOutbound true to enable outbound debugging/logging
   * @param instanceId an ID to be printed with all output
   */
  public FrameDebugHandler(boolean debugInbound, boolean debugOutbound,
      String instanceId) {

    this.instanceId = instanceId == null ? UUID.randomUUID().toString() :
        instanceId;
    this.debugInbound = debugInbound;
    this.debugOutbound = debugOutbound;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws
      Exception {

    if (debugInbound && msg instanceof Frame) {
      System.out.printf("[%s] [%s] Inbound frame: %s\n", formatter.format(
          new Date()), instanceId, msg);
    }

    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {

    if (debugOutbound && msg instanceof Frame) {
      System.out.printf("[%s] [%s] Outbound frame: %s\n", formatter.format(
          new Date()), instanceId, msg);
    }

    super.write(ctx, msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.printf("[%s] [%s] Channel inactive.\n", new Date(), instanceId);
    super.channelInactive(ctx);
  }

}
