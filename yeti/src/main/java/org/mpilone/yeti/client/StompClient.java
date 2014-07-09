package org.mpilone.yeti.client;

import static java.lang.String.format;

import java.util.*;
import java.util.concurrent.*;

import org.mpilone.yeti.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * <p>
 * A simple STOMP client that uses Netty to connect to a remote host. The client
 * builds a pipeline containing an internal {@link StompletFrameHandler} and
 * {@link Stomplet} implementation to delegate all incoming frames to
 * {@link FrameListener}s. An queuing frame listener implementation is provided
 * to support synchronous, pull operations.
 * </p>
 *
 * @author mpilone
 */
public class StompClient {

  private static final int HEARTBEAT_CL_SEND = (int) TimeUnit.SECONDS.toMillis(
      60);
  private static final int HEARTBEAT_CL_RECV = (int) TimeUnit.SECONDS.toMillis(
      60);

  private Channel channel;
  private NioEventLoopGroup workerGroup;

  private String host;
  private int port;

  private final QueuingFrameListener connectedListener;
  private final List<FrameListener> errorListeners;
  private final List<FrameListener> receiptListeners;
  private final Map<String, FrameListener> subscriptionMap;

  /**
   * Constructs the client. The client will not connect until {@link #connect()
   * } is called.
   *
   * @param host the host to connect to
   * @param port the port to connect to
   */
  public StompClient(String host, int port) {
    this(false, host, port);
  }

  /**
   * Constructs the client. The client will not connect until {@link #connect()
   * } is called.
   *
   * @param frameDebugEnabled true to enable frame debugging, false otherwise
   * @param host the host to connect to
   * @param port the port to connect to
   */
  public StompClient(boolean frameDebugEnabled, String host, int port) {
    this.connectedListener = new QueuingFrameListener();
    this.errorListeners = Collections.synchronizedList(
        new ArrayList<FrameListener>());
    this.receiptListeners = Collections.synchronizedList(
        new ArrayList<FrameListener>());
    this.subscriptionMap = new ConcurrentHashMap<>();

    this.port = port;
    this.host = host;
  }

  /**
   * Sets the host to connect to. The value will not be used until the next
   * connect call.
   *
   * @param host the host to connect to
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Sets the port to connect to. The value will not be used until the next
   * connect call.
   *
   * @param port the port to connect to
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Adds a listener to be notified of {@link Command#ERROR} frames.
   *
   * @param listener the listener to be notified
   */
  public void addErrorListener(FrameListener listener) {
    errorListeners.add(listener);
  }

  /**
   * Removes a listener to be notified of {@link Command#ERROR} frames.
   *
   * @param listener the listener to be removed
   */
  public void removeErrorListener(FrameListener listener) {
    errorListeners.remove(listener);
  }

  /**
   * Adds a listener to be notified of {@link Command#RECEIPT} frames.
   *
   * @param listener the listener to be notified
   */
  public void addReceiptListener(FrameListener listener) {
    receiptListeners.add(listener);
  }

  /**
   * Removes a listener to be notified of {@link Command#RECEIPT} frames.
   *
   * @param listener the listener to be removed
   */
  public void removeReceiptListener(FrameListener listener) {
    receiptListeners.remove(listener);
  }

  /**
   * Connects to the remote server by opening a network connection and then
   * immediately sending the STOMP CONNECT frame. When the method returns, the
   * client is fully connected (at the network and STOMP layers) and can
   * immediately begin sending frames or subscribing to destinations.
   *
   * @throws InterruptedException if the connect operation is interrupted
   * @throws StompException if the STOMP CONNECT frame fails
   */
  public void connect() throws InterruptedException, StompException {
    workerGroup = new NioEventLoopGroup();

    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.handler(createHandler());

    // Start the client.
    ChannelFuture f = b.connect(host, port).sync();
    channel = f.channel();

    Frame request = FrameBuilder.connect(StompVersion.VERSION_1_2, host)
        .header(Headers.HEART_BEAT, format("%d,%d", HEARTBEAT_CL_SEND,
                HEARTBEAT_CL_RECV)).build();

    channel.writeAndFlush(request);

    Frame response = connectedListener.poll(10, TimeUnit.SECONDS);
    if (response == null) {
      channel.close().sync();
      throw new StompException("Connect failure: CONNECTED frame not received.");
    }
  }

  /**
   * Creates the channel handler. By default a {@link ChannelInitializer} is
   * created which will construct a pipeline of
   * {@link StompFrameDecoder}, {@link StompFrameEncoder}, {@link FrameDebugHandler},
   * and an internal implementation of {@link StompletFrameHandler}.
   *
   * @return the channel handler for channel (i.e. the server connection)
   * construction
   */
  protected ChannelHandler createHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(StompFrameDecoder.class.getName(),
            new StompFrameDecoder());
        ch.pipeline().addLast(StompFrameEncoder.class.getName(),
            new StompFrameEncoder());
        ch.pipeline().addLast(FrameDebugHandler.class.getName(),
            new FrameDebugHandler());
        ch.pipeline().addLast(StompletFrameHandler.class.getName(),
            new StompletFrameHandler(new DispatchingStomplet()));
      }
    };
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#SEND}.
   *
   * @param frame the frame to write to the remote server
   */
  public void send(Frame frame) {
    validateCommand(frame, Command.SEND);

    channel.writeAndFlush(frame);
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#BEGIN}.
   *
   * @param frame the frame to write to the remote server
   */
  public void begin(Frame frame) {
    validateCommand(frame, Command.BEGIN);

    channel.writeAndFlush(frame);
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#COMMIT}.
   *
   * @param frame the frame to write to the remote server
   */
  public void commit(Frame frame) {
    validateCommand(frame, Command.COMMIT);

    channel.writeAndFlush(frame);
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#ABORT}.
   *
   * @param frame the frame to write to the remote server
   */
  public void abort(Frame frame) {
    validateCommand(frame, Command.ABORT);

    channel.writeAndFlush(frame);
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#SUBSCRIBE}.
   *
   * @param frame the frame to write to the remote server
   * @param listener the listener to be notified of messages received on this
   * subscription
   */
  public void subscribe(Frame frame, FrameListener listener) {
    validateCommand(frame, Command.SUBSCRIBE);

    channel.writeAndFlush(frame);
    subscriptionMap.put(frame.getHeaders().get(Headers.ID), listener);
  }

  /**
   * Writes the given frame to the remote server. The frame command must be
   * {@link Command#UNSUBSCRIBE}.
   *
   * @param frame the frame to write to the remote server
   */
  public void unsubscribe(Frame frame) {
    validateCommand(frame, Command.UNSUBSCRIBE);

    channel.writeAndFlush(frame);
    subscriptionMap.remove(frame.getHeaders().get(Headers.ID));
  }

  /**
   * Validates that the command in the frame matches the required command.
   *
   * @param frame the frame to validate
   * @param requiredCommand the required command
   *
   * @throws IllegalArgumentException if the frame command does not match
   */
  private void validateCommand(Frame frame, Command requiredCommand) throws
      IllegalArgumentException {
    if (frame.getCommand() != requiredCommand) {
      throw new IllegalArgumentException(format("A %s command is required.",
          requiredCommand));
    }
  }

  /**
   * Returns true if the client is currently connected to the server and can
   * send and receive frames.
   *
   * @return true if the client is connected
   */
  public boolean isConnected() {
    return channel != null && channel.isActive();
  }

  /**
   * Disconnects from the remote server by issuing a {@link Command#DISCONNECT}
   * frame and closing the network connection. This method blocks until the
   * disconnect it complete. A receipt will be requested but the specification
   * indicates that a server may not issue one on a disconnect request.
   *
   * @throws InterruptedException if the disconnect is interrupted
   */
  public void disconnect() throws InterruptedException {

    if (channel.isActive()) {
      // Request a receipt even though we're never going to read it.
      Frame frame = FrameBuilder.disconnect().header(Headers.RECEIPT,
          "receipt-" + UUID.randomUUID().toString()).build();

      channel.writeAndFlush(frame).sync();

      closeAndShutdown();
    }
  }

  /**
   * Closes the underlying channel and shutsdown the worker group. This method
   * is normally called automatically by the {@link #disconnect() } method after
   * sending the disconnect frame.
   *
   * @throws InterruptedException if the close is interrupted
   */
  protected void closeAndShutdown() throws InterruptedException {
    try {
      channel.close().sync();
    }
    finally {
      try {
        workerGroup.shutdownGracefully().get(10, TimeUnit.SECONDS);
      }
      catch (ExecutionException | TimeoutException ex) {
        // ignore
      }

      workerGroup = null;
      channel = null;
    }
  }

  /**
   * An internal Stomplet that dispatches incoming frames to
   * {@link FrameListener}s.
   */
  private class DispatchingStomplet extends ClientStomplet {

    @Override
    protected void doConnected(StompletRequest req, StompletResponse res) throws
        Exception {

      // Defaults.
      int clSend = HEARTBEAT_CL_SEND; // cx
      int clRecv = HEARTBEAT_CL_RECV; // cy
      int srSend = 0; // sx
      int srRecv = 0; // sy

      // Parse the heartbeat header to get the server's values.
      Frame frame = req.getFrame();
      String heartbeat = frame.getHeaders().get(Headers.HEART_BEAT);
      if (heartbeat != null) {
        String[] values = heartbeat.trim().split(",");
        if (values.length == 2) {
          try {
            srSend = Integer.parseInt(values[0]);
            srRecv = Integer.parseInt(values[1]);
          }
          catch (NumberFormatException ex) {
            throw new StompException(format("Invalid %s header value %s.",
                Headers.HEART_BEAT, heartbeat), null, frame, ex);
          }
        }
      }

      int clientToServerInterval = srRecv == 0 ? 0 : Math.max(clSend, srRecv);
      int serverToClientInterval = srSend == 0 ? 0 : Math.max(clRecv, srSend);

      // Init the heartbeat in the stomplet context.
      getStompletContext().configureHeartbeat(serverToClientInterval,
          clientToServerInterval);

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

  /**
   * A frame listener to be notified when a frame is received.
   */
  public interface FrameListener {

    /**
     * Handles a received frame.
     *
     * @param frame the frame received
     *
     * @throws Exception if there is an error handling the frame
     */
    void frameReceived(Frame frame) throws Exception;
  }

  /**
   * A frame listener that queues any frames received in an internal
   * {@link BlockingQueue} to allow the frames to be received synchronously
   * through a pull operation.
   */
  public static class QueuingFrameListener implements FrameListener {

    /**
     * The frame queue that will buffer incoming frames until they are consumed
     * via the pull API.
     */
    private final BlockingQueue<Frame> frameQueue;

    /**
     * Constructs the listener with a queue size of 50. If frames arrive faster
     * than they are consumed, new frames will be dropped.
     */
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
