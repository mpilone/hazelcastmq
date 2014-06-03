
package org.mpilone.stomp.client;

import static java.lang.String.format;

import java.util.*;
import java.util.concurrent.*;

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
public class StompClient {

  private Channel channel;
  private NioEventLoopGroup workerGroup;

  private String host;
  private int port;
  private boolean frameDebugEnabled;

  private final QueuingFrameListener connectedListener;
  private final List<FrameListener> errorListeners;
  private final List<FrameListener> receiptListeners;
  private final Map<String, FrameListener> subscriptionMap;

  public StompClient() {
    this.connectedListener = new QueuingFrameListener();
    this.errorListeners = Collections.synchronizedList(
        new ArrayList<FrameListener>());
    this.receiptListeners = Collections.synchronizedList(
        new ArrayList<FrameListener>());
    this.subscriptionMap = new ConcurrentHashMap<>();
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setFrameDebugEnabled(boolean frameDebugEnabled) {
    this.frameDebugEnabled = frameDebugEnabled;
  }

  public void addErrorListener(FrameListener listener) {
    errorListeners.add(listener);
  }

  public void removeErrorListener(FrameListener listener) {
    errorListeners.remove(listener);
  }

  public void addReceiptListener(FrameListener listener) {
    receiptListeners.add(listener);
  }

  public void removeReceiptListener(FrameListener listener) {
    receiptListeners.remove(listener);
  }

  public void connect() throws InterruptedException {
    workerGroup = new NioEventLoopGroup();

    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.handler(createHandler());

    // Start the client.
    ChannelFuture f = b.connect(host, port).sync();
    channel = f.channel();

    channel.writeAndFlush(FrameBuilder.connect("1.2", host).build());

    Frame response = connectedListener.poll(10, TimeUnit.SECONDS);
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

        if (frameDebugEnabled) {
          ch.pipeline().addLast(new FrameDebugHandler());
        }
        ch.pipeline().addLast(
            new StompletFrameHandler(new DispatchingStomplet()));
      }
    };
  }

  public void send(Frame frame) {
    validateCommand(frame, Command.SEND);

    channel.writeAndFlush(frame);
  }

  public void begin(Frame frame) {
    validateCommand(frame, Command.BEGIN);

    channel.writeAndFlush(frame);
  }

  public void commit(Frame frame) {
    validateCommand(frame, Command.COMMIT);

    channel.writeAndFlush(frame);
  }

  public void abort(Frame frame) {
    validateCommand(frame, Command.ABORT);

    channel.writeAndFlush(frame);
  }

  public void subscribe(Frame frame, FrameListener listener) {
    validateCommand(frame, Command.SUBSCRIBE);

    channel.writeAndFlush(frame);
    subscriptionMap.put(frame.getHeaders().get(Headers.ID), listener);
  }

  public void unsubscribe(Frame frame) {
    validateCommand(frame, Command.UNSUBSCRIBE);

    channel.writeAndFlush(frame);
    subscriptionMap.remove(frame.getHeaders().get(Headers.ID));
  }

  private void validateCommand(Frame frame, Command requiredCommand) {
    if (frame.getCommand() != requiredCommand) {
      throw new IllegalArgumentException(format("A %s command is required.",
          requiredCommand));
    }
  }

  public void disconnect() throws InterruptedException {

    if (channel.isActive()) {
      try {
        // Request a receipt even though we're never going to read it.
        Frame frame = FrameBuilder.disconnect().header(Headers.RECEIPT,
            "receipt-" + UUID.randomUUID().toString()).build();

        channel.writeAndFlush(frame).sync();
        channel.close().sync();
      }
      finally {
        workerGroup.shutdownGracefully();

        workerGroup = null;
        channel = null;
      }
    }
  }

  private class DispatchingStomplet extends ClientStomplet {

    @Override
    protected void doConnected(StompletRequest req, StompletResponse res) throws
        Exception {
      connectedListener.frameReceived(req.getFrame());
    }

    @Override
    protected void doError(StompletRequest req, StompletResponse res) throws
        Exception {
      synchronized (errorListeners) {
        for (FrameListener listener : errorListeners) {
          listener.frameReceived(req.getFrame());
        }
      }
    }

    @Override
    protected void doMessage(StompletRequest req, StompletResponse res) throws
        Exception {

      Frame frame = req.getFrame();
      String subId = frame.getHeaders().get(Headers.SUBSCRIPTION);
      FrameListener listener = subscriptionMap.get(subId);

      if (listener != null) {
        listener.frameReceived(frame);
      }
    }

    @Override
    protected void doReceipt(StompletRequest req, StompletResponse res) throws
        Exception {
      synchronized (receiptListeners) {
        for (FrameListener listener : receiptListeners) {
          listener.frameReceived(req.getFrame());
        }
      }
    }
  }

  public interface FrameListener {
    void frameReceived(Frame frame) throws Exception;
  }

  public static class QueuingFrameListener implements FrameListener {

    /**
     * The frame queue that will buffer incoming frames until they are consumed
     * via the pull API.
     */
    private final BlockingQueue<Frame> frameQueue;

    public QueuingFrameListener() {
      this.frameQueue = new ArrayBlockingQueue<>(50);
    }

    @Override
    public void frameReceived(Frame frame) throws Exception {
      if (!frameQueue.offer(frame, 2, TimeUnit.SECONDS)) {
        System.out.println(
            "Unable to queue incoming frame. The max frame queue count or "
            + "message consumption performance must increase. Frames will "
            + "be lost.");
      }
    }

    /**
     * Receives the next available frame if there is one, otherwise the method
     * returns null immediately.
     *
     * @return the available frame or null
     */
    public Frame poll() {
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
    public Frame poll(long timeout, TimeUnit unit) throws
        InterruptedException {
      return frameQueue.poll(timeout, unit);
    }
  }
}
