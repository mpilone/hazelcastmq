package org.mpilone.hazelcastmq.stomp.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.mpilone.hazelcastmq.core.HazelcastMQInstance;

/**
 * The configuration of the stomper server.
 * 
 * @author mpilone
 */
public class HazelcastMQStompServerConfig {
  /**
   * The port to which the server will bind to listen for incoming connections.
   */
  private int port;

  /**
   * The JMS connection factory to use for all message consumers and producers.
   */
  private HazelcastMQInstance hazelcastMQInstance;

  /**
   * The executor service to spin up the server and client threads.
   */
  private ExecutorService executor;

  /**
   * The frame converter used to convert STOMP frames into JMS messages.
   */
  private FrameConverter frameConverter;

  /**
   * Constructs a configuration which will not have a connection factory set. A
   * connection factory must be set before constructing a stomper instance.
   */
  public HazelcastMQStompServerConfig() {
    this(null);
  }

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>frameConverter: {@link DefaultFrameConverter}</li>
   * <li>executor: {@link Executors#newCachedThreadPool()}</li>
   * </ul>
   * 
   * @param connectionFactory
   *          the JMS connection factory to use for all message consumers and
   *          producers
   */
  public HazelcastMQStompServerConfig(HazelcastMQInstance connectionFactory) {
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
   * Returns the port to which the server will bind to listen for incoming
   * connections.
   * 
   * @param port
   *          the port number
   */
  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  /**
   * Returns the JMS connection factory to use for all message consumers and
   * producers.
   * 
   * @return the connection factory to access JMS
   */
  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * @param connectionFactory
   *          the connectionFactory to set
   */
  public void setHazelcastMQInstance(HazelcastMQInstance connectionFactory) {
    this.hazelcastMQInstance = connectionFactory;
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
   * @param frameConverter
   *          the frameConverter to set
   */
  public void setFrameConverter(FrameConverter frameConverter) {
    this.frameConverter = frameConverter;
  }
}
