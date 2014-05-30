package org.mpilone.stomp.client;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mpilone.stomp.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 *
 * @author mpilone
 */
public class BasicStompClient {

  private Channel channel;
  private NioEventLoopGroup workerGroup;
  private FrameListener frameListener;
  private final static AtomicInteger RECEIPT_COUNT = new AtomicInteger();

  /**
   * The frame queue that will buffer incoming frames until they are consumed
   * via the pull API.
   */
  private BlockingQueue<Frame> frameQueue;

  public void connect(String host, int port) throws InterruptedException {

    workerGroup = new NioEventLoopGroup();

    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.handler(createHandler());

    this.frameQueue = new ArrayBlockingQueue<Frame>(100);

    // Start the client.
    ChannelFuture f = b.connect(host, port).sync();
    channel = f.channel();

    Frame frame = FrameBuilder.connect("1.2", host).build();
    channel.writeAndFlush(frame);

    Frame response = receive(10, TimeUnit.SECONDS);
    if (response == null || response.getCommand() != Command.CONNECTED) {
      throw new StompClientException(
          "Unexpected frame returned while connecting: " + response);
    }
  }

  protected ChannelHandler createHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new StompFrameDecoder());
        ch.pipeline().addLast(new StompFrameEncoder());

        ch.pipeline().addLast(new FrameDebugHandler());
        ch.pipeline().addLast(new BasicClientFrameHandler());
      }
    };
  }

  /**
   * Receives the next available frame if there is one, otherwise the method
   * returns null immediately.
   *
   * @return the available frame or null
   */
  public Frame receiveNoWait() {
    return frameQueue.poll();
  }

  /**
   * Receives the next available frame, blocking up to the given timeout. Null
   * is returned if no frame is available before the timeout.
   *
   * @param timeout the maximum time to block waiting for a frame
   * @param unit the time unit
   *
   * @return the frame or null
   * @throws InterruptedException if there is an error waiting for a frame
   */
  public Frame receive(long timeout, TimeUnit unit) throws InterruptedException {
      return frameQueue.poll(timeout, unit);
  }

  /**
   * Sets the frame listener to receive frames as soon as they arrive. Only a
   * single thread services the listener so if the frame takes a long time to
   * process, it should be handled in a separate thread. If this client was
   * already subscribed and has been receiving messages, the messages could be
   * lost when the listener is set. To remove the listener, set it to null.
   *
   * @param frameListener the frame listener to set or null to clear it
   */
  public void setFrameListener(FrameListener frameListener) {
    this.frameListener = frameListener;

    // It is possible that we could lose frames here if the client was already
    // receiving messages before setting the listener; however this isn't a
    // supported operation.
    frameQueue.clear();
  }

  public void write(Frame frame) {
    channel.writeAndFlush(frame);
  }

  public void disconnect() throws InterruptedException {

    frameListener = null;

    if (channel.isActive()) {
      try {
        Frame frame = FrameBuilder.disconnect().header(Headers.RECEIPT,
            "receipt-" + RECEIPT_COUNT.incrementAndGet()).build();
        channel.writeAndFlush(frame).sync();

        frame = receive(5, TimeUnit.SECONDS);

        if (frame == null || frame.getCommand() != Command.RECEIPT) {
          // TODO: raise an error?
        }

        channel.close().sync();
      }
      finally {
        workerGroup.shutdownGracefully();

        workerGroup = null;
        channel = null;
      }
    }
  }

  public interface FrameListener {
    void frameReceived(Frame frame);
  }

  private class BasicClientFrameHandler extends SimpleChannelInboundHandler<Frame> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
        Exception {

      try {
        if (frameListener != null) {
          frameListener.frameReceived(frame);
        }
      // Queue it up if there is no listener and assume that someone is going to
        // dequeue it.
        else if (!frameQueue.offer(frame, 2, TimeUnit.SECONDS)) {
          System.out.println(
              "Unable to queue incoming frame. The max frame queue count or "
              + "message consumption performance must increase. Frames will "
              + "be lost.");
        }
      }
      catch (Throwable ex) {
        // Ignore and wait for the next frame.
      }

      ctx.fireChannelRead(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);
    }
  }

}
