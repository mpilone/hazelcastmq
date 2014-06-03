
package org.mpilone.stomp;

import static org.mpilone.stomp.StompConstants.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
public class StompletFrameHandler extends SimpleChannelInboundHandler<Frame> {

  private final Stomplet stomplet;

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
    // TODO: add support for setting up the heartbeat via the stomplet context.
    stomplet.init(new Stomplet.StompletContext() {
    });
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
      cause.printStackTrace();
    }

    fb.headerContentTypeText();
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

  protected class StompletRequestImpl implements Stomplet.StompletRequest {

    private final Frame frame;

    public StompletRequestImpl(Frame frame) {
      this.frame = frame;
    }

    @Override
    public Frame getFrame() {
      return frame;
    }
  }

  protected class StompletResponseImpl implements Stomplet.StompletResponse {

    private boolean finalResponse = false;
    private final Stomplet.WritableFrameChannel writableFrameChannel;

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

    public boolean isFinalResponse() {
      return finalResponse;
    }
  }
}
