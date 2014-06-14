
package org.mpilone.yeti;

import static java.lang.String.format;

import java.text.SimpleDateFormat;
import java.util.*;

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
      logMessage(format("[%s] Inbound frame: %s", instanceId, msg));
    }

    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {

    if (debugOutbound && msg instanceof Frame) {
      logMessage(format("[%s] Outbound frame: %s", instanceId, msg));
    }

    super.write(ctx, msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logMessage(format("[%s] Channel inactive.", instanceId));
    super.channelInactive(ctx);
  }

  /**
   * Prints the given message to the logging system. The message will include
   * the instance ID and any frame details. By default, this will be stdout.
   * Subclasses should overload this method to write to a different logging
   * system.
   *
   * @param msg the message to log
   */
  protected void logMessage(String msg) {
    System.out.printf("[%s] %s\n", formatter.format(new Date()), msg);
  }

}
