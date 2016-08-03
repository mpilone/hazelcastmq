package org.mpilone.hazelcastmq.stomp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.mpilone.hazelcastmq.core.Broker;
import org.mpilone.yeti.StompFrameDecoder;

/**
 * The configuration of the STOMP server.
  * 
 * @author mpilone
 */
public class StompAdapterConfig {

  private int maxFrameSize;
  private int port;
  private Broker broker;
  private FrameConverter frameConverter;
  private ExecutorService executor;

  /**
   * Constructs a configuration which will not have an MQ broker or executor
   * set. A broker and executor must be set before constructing a
   * {@link HazelcastMQStomp} instance.
   */
  public StompAdapterConfig() {
    this(null);
  }

  public StompAdapterConfig(Broker broker) {
    this(broker, null);
  }

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>frameConverter: {@link BasicFrameConverter}</li>
   * <li>frameDebugEnabled: false</li>
   * <li>maxFrameSize: {@link StompFrameDecoder#DEFAULT_MAX_FRAME_SIZE}</li>
   * </ul>
   * 
   * @param broker the HzMq broker to use for all message channels
   * @param executor the executor used to dispatch subscription messages to
   * connected clients
   *
   */
  public StompAdapterConfig(Broker broker, ExecutorService executor) {
    this.broker = broker;
    this.executor = executor;

    frameConverter = new BasicFrameConverter();
    port = 8032;
    maxFrameSize = StompFrameDecoder.DEFAULT_MAX_FRAME_SIZE;
  }

  /**
   * Returns the executor used to dispatch subscription messages to connected
   * clients.
   *
   * @return the executor instance
   */
  public ExecutorService getExecutor() {
    if (executor == null) {
      executor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicLong counter = new AtomicLong();
        private final ThreadFactory delegate = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
          Thread t = delegate.newThread(r);
          t.setName("hazelcastmq-stomp-" + counter.incrementAndGet());
          t.setDaemon(true);
          return t;
        }
      });
    }

    return executor;
  }

  /**
   * Sets the executor used to dispatch subscription messages to connected
   * clients. The maximum number of threads in the executor must be greater than
   * the maximum number of client subscriptions or clients may block.
   *
   * @param executor the executor instance
   */
  public void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Sets the maximum supported frame size in bytes. Frames larger than this
   * size will be considered invalid and an invalid frame will be generated.
   *
   * @param maxFrameSize the maximum frame size in bytes
   */
  public void setMaxFrameSize(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

  /**
   * Returns the maximum supported frame size in bytes. Frames larger than this
   * size will be considered invalid and an invalid frame will be generated.
   *
   * @return the maximum frame size in bytes
   */
  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  /**
   * Sets the port to which the server will bind to listen for incoming
   * connections.
   *
   * @param port the port number
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the port to which the server will bind to listen for incoming
   * connections.
   *
   * @return the port number
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the MQ broker to use for all message channels.
    * 
   * @return the MQ broker
   */
  public Broker getBroker() {
    return broker;
  }

  /**
   * Sets the MQ broker to use for all message channels.
   *
   * @param broker the MQ broker
   */
  public void setBroker(Broker broker) {
    this.broker = broker;
  }

  /**
   * The frame converter used to convert STOMP frames into HazelcastMQ messages.
    * 
   * @return the frame converter
   */
  public FrameConverter getFrameConverter() {
    return frameConverter;
  }

  /**
   * Sets the the frame converter used to convert STOMP frames into HazelcastMQ
   * messages.
   *
   * @param frameConverter
   *          the frameConverter to set
   */
  public void setFrameConverter(FrameConverter frameConverter) {
    this.frameConverter = frameConverter;
  }
}
