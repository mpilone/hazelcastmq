package org.mpilone.hazelcastmq.stomper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.ConnectionFactory;

public class HazelcastMQStomperConfig {
  private int port;
  private ConnectionFactory connectionFactory;
  private ExecutorService executor;
  private FrameConverter frameConverter;

  public HazelcastMQStomperConfig() {
    frameConverter = new DefaultFrameConverter();
    port = 8032;
    executor = Executors.newCachedThreadPool();
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  /**
   * @return the connectionFactory
   */
  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  /**
   * @param connectionFactory
   *          the connectionFactory to set
   */
  public void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  /**
   * @return the executor
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
   * @return the frameConverter
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
