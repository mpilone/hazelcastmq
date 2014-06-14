
package org.mpilone.yeti;

import static org.mpilone.yeti.StompConstants.UTF_8;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.handler.timeout.*;

/**
 * <p>
 * A channel handler that manages the lifecycle of a {@link Stomplet} and
 * delegates all incoming frames to the stomplet via the
 * {@link Stomplet#service(org.mpilone.yeti.Stomplet.StompletRequest, org.mpilone.yeti.Stomplet.StompletResponse)}
 * method. This is a terminal channel handler so it should appear last in the
 * pipeline.
 * </p>
 * <p>
 * Any exceptions raised by the stomplet (or pipeline) will be relayed to the
 * remote connection as an ERROR frame. If the exception implements
 * {@link StompClientException} the message and details of the exception will be
 * used in the frame otherwise a generic "Internal Server Error" message will be
 * used.
 * </p>
 *
 * @author mpilone
 */
public class StompletFrameHandler extends SimpleChannelInboundHandler<Frame> {

  /**
   * The stomplet that all frames will be delegated to.
   */
  private final Stomplet stomplet;

  /**
   * Constructs the handler which will delegate all frame handling to the
   * Stomplet.
   *
   * @param stomplet the stomp that will service all requests
   */
  public StompletFrameHandler(Stomplet stomplet) {
    super(Frame.class, true);

    this.stomplet = stomplet;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame msg) throws
      Exception {

    StompletRequestImpl req = new StompletRequestImpl(msg);
    StompletResponseImpl res = new StompletResponseImpl(ctx.channel());
    stomplet.service(req, res);

    if (res.isFinalResponse()) {
      ctx.close();
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    stomplet.init(new StompletContextImpl(ctx.channel()));
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    stomplet.destroy();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws
      Exception {

    FrameBuilder fb = FrameBuilder.command(Command.ERROR);
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
    }

    fb.headerContentTypeText();
    fb.headerContentLength();

      ctx.writeAndFlush(fb.build());
      ctx.close();
  }

  /**
   * Wraps the given extension looking for a {@link StompClientException} to
   * determine if the error message and details should be relayed to the client.
   *
   * @param ex the exception to unwrap
   *
   * @return the stomp client exception or null if one is not found in the
   * exception stack
   */
  private StompClientException unwrapClientException(Throwable ex) {
    if (ex == null) {
      return null;
    }
    else if (ex instanceof StompClientException) {
      return (StompClientException) ex;
    }
    else {
      return unwrapClientException(ex.getCause());
    }
  }

  /**
   * A channel handler that detects when the channel has not performed a read or
   * write for a while. This is just a simple tagging extension for easy
   * addition/removal from the pipeline.
   */
  private static class HeartbeatIdleHandler extends IdleStateHandler {

    /**
     * Constructs the handler.
     *
     * @param readerIdleTime the amount of time before a reader idle event is
     * generated in milliseconds or 0 to disable
     * @param writerIdleTime the amount of time before a writer idle event is
     * generated in milliseconds or 0 to disable
     */
    public HeartbeatIdleHandler(int readerIdleTime, int writerIdleTime) {
      super(readerIdleTime, writerIdleTime, 0, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * A channel handler that listens for idle events from the
   * {@link HeartbeatIdleHandler} and response appropriately. If the read thread
   * is idle, the client will be assumed dead and the connection will be closed.
   * If the write thread is idle, a new-line character will be sent.
   */
  private static class HeartbeatEventHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws
        Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE) {
          //System.out.println(this + " READER_IDLE");

          // Assume the remote host is dead. Close the connection.
          ctx.close();
        }
        else if (e.state() == IdleState.WRITER_IDLE) {
          //System.out.println(this + " WRITER_IDLE");

          // Send a single new-line character to keep the connection alive.
          ByteBuf out = Unpooled.buffer(1);
          out.writeByte(StompConstants.LINE_FEED_CHAR);
          ctx.writeAndFlush(out);
        }
      }
    }
  }

  /**
   * The implementation of the stomplet context.
   */
  protected class StompletContextImpl implements Stomplet.StompletContext {

    private final Channel channel;

    /**
     * Constructs the context.
     *
     * @param channel the channel handling all IO for the somplet.
     */
    public StompletContextImpl(Channel channel) {
      this.channel = channel;
    }

    @Override
    public void configureHeartbeat(int readInterval, int writeInterval) {

      // Remove any previous heartbeat configuration.
      ChannelPipeline pipeline = channel.pipeline();
      if (pipeline.get(HeartbeatEventHandler.class) != null) {
        pipeline.remove(HeartbeatEventHandler.class);
      }
      if (pipeline.get(HeartbeatIdleHandler.class) != null) {
        pipeline.remove(HeartbeatIdleHandler.class);
      }

      // Create the new handlers. We allow +/- 10% to cover network lag in
      // the heartbeat messages.
      ChannelHandler idleHandler = new HeartbeatIdleHandler((int) (readInterval
          * 1.1), (int) (writeInterval * .9));
      ChannelHandler eventHandler = new HeartbeatEventHandler();

      // Add the new handlers to the head of the
      pipeline.addFirst(HeartbeatEventHandler.class.getName(), eventHandler);
      pipeline.addFirst(HeartbeatIdleHandler.class.getName(), idleHandler);
    }
  }

  /**
   * The implementation of the stomplet request.
   */
  private class StompletRequestImpl implements Stomplet.StompletRequest {

    private final Frame frame;

    /**
     * Constructs the request.
     *
     * @param frame the frame to be serviced
     */
    public StompletRequestImpl(Frame frame) {
      this.frame = frame;
    }

    @Override
    public Frame getFrame() {
      return frame;
    }
  }

  /**
   * The implementation of the stomplet response.
   */
  private class StompletResponseImpl implements Stomplet.StompletResponse {

    private boolean finalResponse = false;
    private final Stomplet.WritableFrameChannel writableFrameChannel;

    /**
     * Constructs the response which will operate on the given channel.
     *
     * @param channel the underlying channel
     */
    public StompletResponseImpl(final Channel channel) {
      this.writableFrameChannel = new Stomplet.WritableFrameChannel() {

        @Override
        public void write(Frame frame) {
          channel.writeAndFlush(frame);
        }
      };
    }

    @Override
    public Stomplet.WritableFrameChannel getFrameChannel() {
      return writableFrameChannel;
    }

    @Override
    public void setFinalResponse(boolean finalResponse) {
      this.finalResponse = finalResponse;
    }

    /**
     * Returns true if this should be the final response and the remote
     * connection should be closed.
     *
     * @return true to close the connection
     */
    public boolean isFinalResponse() {
      return finalResponse;
    }
  }
}
