package org.mpilone.hazelcastmq.stomp.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.mpilone.hazelcastmq.core.HazelcastMQInstance;

/**
 * The configuration of the STOMP server.
  * 
 * @author mpilone
 */
public class HazelcastMQStompConfig {
  private int port;
  private boolean frameDebugEnabled;
  private HazelcastMQInstance hazelcastMQInstance;
  private ExecutorService executor; // may not be needed anymore
  private FrameConverter frameConverter;

  /**
   * Constructs a configuration which will not have an MQ instance set. A MQ
   * instance must be set before constructing a {@link HazelcastMQStomp}
   * instance.
   */
  public HazelcastMQStompConfig() {
    this(null);
  }

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>frameConverter: {@link DefaultFrameConverter}</li>
   * <li>executor: {@link Executors#newCachedThreadPool()}</li>
   * <li>frameDebugEnabled: false</li>
   * </ul>
   * 
   * @param connectionFactory
   *          the JMS connection factory to use for all message consumers and
   *          producers
   */
  public HazelcastMQStompConfig(HazelcastMQInstance connectionFactory) {
    this.hazelcastMQInstance = connectionFactory;

    frameConverter = new DefaultFrameConverter();
    port = 8032;
    executor = Executors.newCachedThreadPool(new ThreadFactory() {

      private AtomicLong counter = new AtomicLong();
      private ThreadFactory delegate = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = delegate.newThread(r);
        t.setName("hazelcastmq-stomper-" + counter.incrementAndGet());
        t.setDaemon(true);
        return t;
      }
    });
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
   * Sets the flag which indicates if frame debugging is enabled at the STOMP
   * frame handling level. The default is false.
   *
   * @param frameDebugEnabled true to enable
   */
  public void setFrameDebugEnabled(boolean frameDebugEnabled) {
    this.frameDebugEnabled = frameDebugEnabled;
  }

  /**
   * Returns the flag which indicates if frame debugging is enabled at the STOMP
   * frame handling level. The default is false.
   *
   * @return true if enabled
   */
  public boolean isFrameDebugEnabled() {
    return frameDebugEnabled;
  }

  /**
   * Returns the MQ instance to use for all message consumers and   * producers.
   * 
   * @return the MQ instance
   */
  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * Sets the MQ instance to use for all message consumers and producers.
   *
   * @param mqInstance the MQ instance
   */
  public void setHazelcastMQInstance(HazelcastMQInstance mqInstance) {
    this.hazelcastMQInstance = mqInstance;
  }

  /**
   * Returns executor service to spin up the server and client threads.
   * 
   * @return the executor to use for all server and client threads
   */
  public ExecutorService getExecutor() {
    return executor;
  }

  /**
   * Sets executor service to spin up the server and client threads.
   *
   * @param executor
   *          the executor to set
   */
  public void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * The frame converter used to convert STOMP frames into JMS messages.
   * 
   * @return the frame converter
   */
  public FrameConverter getFrameConverter() {
    return frameConverter;
  }

  /**
   * Sets the the frame converter used to convert STOMP frames into JMS
   * messages.
   *
   * @param frameConverter
   *          the frameConverter to set
   */
  public void setFrameConverter(FrameConverter frameConverter) {
    this.frameConverter = frameConverter;
  }
}
