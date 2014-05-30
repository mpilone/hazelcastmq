package org.mpilone.stomp.client;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mpilone.stomp.shared.*;

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
  private final Object waiter = new Object();
  private boolean connected = false;
  private Frame receivedFrame;
  private MessageFrameListener messageFrameListener;
  private final static AtomicInteger RECEIPT_COUNT = new AtomicInteger();

  public Frame connect(String host, int port) throws InterruptedException {

    receivedFrame = null;
    workerGroup = new NioEventLoopGroup();

    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.handler(createHandler());

    // Start the client.
    ChannelFuture f = b.connect(host, port).sync();
    channel = f.channel();

    Frame frame = FrameBuilder.connect("1.2", host).build();
    channel.writeAndFlush(frame);
    sleep();

    if (!connected) {
      System.out.println("Expected to be connected but wasn't!");
    }

    return receivedFrame;
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

  public void setMessageFrameListener(MessageFrameListener messageFrameListener) {
    this.messageFrameListener = messageFrameListener;
  }

  private void sleep() {
    synchronized (waiter) {
      try {
        waiter.wait(TimeUnit.SECONDS.toMillis(5));
//        waiter.wait(TimeUnit.MINUTES.toMillis(5));
      }
      catch (InterruptedException ex) {
        throw new RuntimeException("Interrupted while sleeping.");
      }
    }
  }

  private void wake() {
    synchronized (waiter) {
      waiter.notify();
    }
  }

  public Frame write(Frame frame, boolean waitForResponse) {
    receivedFrame = null;

    channel.writeAndFlush(frame);
    if (waitForResponse) {
      sleep();
    }

    return receivedFrame;
  }

  public Frame disconnect() throws InterruptedException {
    receivedFrame = null;

    if (channel.isActive()) {
      try {
        Frame frame = FrameBuilder.disconnect().header(Headers.RECEIPT,
            "receipt-" + RECEIPT_COUNT.incrementAndGet()).build();
        channel.writeAndFlush(frame);
        sleep();

        // Wait until the connection is closed.
        channel.close().sync();
      }
      finally {
        workerGroup.shutdownGracefully();

        workerGroup = null;
        channel = null;
      }
    }

    return receivedFrame;
  }

  public interface MessageFrameListener {
    void onMessageFrameReceived(Frame frame);
  }

  private class BasicClientFrameHandler extends SimpleChannelInboundHandler<Frame> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
        Exception {

      switch (frame.getCommand()) {
        case CONNECTED:
          connected = true;
          receivedFrame = frame;
          break;

        case RECEIPT:
          receivedFrame = frame;
          break;

        case ERROR:
          receivedFrame = frame;
          break;

        case MESSAGE:
          if (messageFrameListener != null) {
            messageFrameListener.onMessageFrameReceived(frame);
          }
          break;

        default:
          System.out.println("Unexpected frame: " + frame);
          break;
      }

      ctx.fireChannelRead(frame);
      wake();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      connected = false;
      super.channelInactive(ctx);
    }
  }

}
