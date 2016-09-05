package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.TimeUnit;

/**
 * The configuration for the broker.
 *
 * @author mpilone
 */
public class BrokerConfig {

  /**
   * The default inflight timeout in seconds.
   */
  public static final int DEFAULT_INFLIGHT_TIMEOUT = (int) TimeUnit.MINUTES
      .toSeconds(10);

  /**
   * The default router executor name.
   */
  public static final String DEFAULT_ROUTING_EXECUTOR_NAME
      = "hzmq.routerexecutor";

  // Possible future config options:
  // - multiple thread router executor
  // - nack destination channel
  // - in-flight timeout
  //
  //
  private HazelcastInstance hazelcastInstance;
  private MessageConverter messageConverter = new NoOpMessageConverter();
  private int inflightTimeout = DEFAULT_INFLIGHT_TIMEOUT;
  private String routerExecutorName = DEFAULT_ROUTING_EXECUTOR_NAME;


  /**
   * Constructs the configuration with the following defaults:
   * <ul>
   * <li>messageConverter: {@link NoOpMessageConverter}</li>
   * <li>hazelcastInstance: {@link Hazelcast#newHazelcastInstance()} (lazy
   * initialized)</li>
   * </ul>
   */
  public BrokerConfig() {
  }

  /**
   * Constructs the configuration with the given Hazelcast instance.
   *
   * @param hazelcastInstance the Hazelcast instance
   */
  public BrokerConfig(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * Returns the Hazelcast instance used to access all distributed objects. If
   * the instance hasn't been set, a new instance will be created.
   *
   * @return the Hazelcast instance
   */
  public HazelcastInstance getHazelcastInstance() {
    if (hazelcastInstance == null) {
      hazelcastInstance = Hazelcast.newHazelcastInstance();
    }

    return hazelcastInstance;
  }

  /**
   * Sets the Hazelcast instance to use to access all distributed objects.
   *
   * @param hazelcastInstance the Hazelcast instance
   */
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * Returns the message converter to use for converter HazelcastMQ messages
   * into and out of the objects/data sent into Hazelcast. The default is the
   * {@link NoOpMessageConverter} so the original {@link Message} is simply
   * passed through to Hazelcast for internal serialization.
   *
   * @return the message converter
   */
  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

  /**
   * Sets the message converter to use for converter HazelcastMQ messages into
   * and out of the objects/data sent into Hazelcast.
   *
   * @param messageConverter the message converter
   */
  public void setMessageConverter(MessageConverter messageConverter) {
    this.messageConverter = messageConverter;
  }

  /**
   * Sets the inflight message timeout in seconds. If a message's inflight time
   * is greater than this timeout, the message will be automatically nacked
   * which normally means the message will be resent on the original channel.
   * This value applies to all channels of this broker. The timeout is normally
   * only checked periodically when there is activity in the broker so the
   * actual timeout may be slightly longer.
   *
   * @param inflightTimeout the inflight timeout in seconds
   */
  public void setInflightTimeout(int inflightTimeout) {
    this.inflightTimeout = inflightTimeout;
  }

  /**
   * Returns the inflight message timeout in seconds.
   *
   * @return the inflight timeout in seconds
   */
  public int getInflightTimeout() {
    return inflightTimeout;
  }

  /**
   * Sets the name of the executor used for executing routing tasks. A task is
   * submitted to the router whenever a message is sent that needs routing.
   *
   * @param routerExecutorName the name of the routing executor
   */
  public void setRouterExecutorName(String routerExecutorName) {
    this.routerExecutorName = routerExecutorName;
  }

  /**
   * Returns the name of the executor used for executing routing tasks.
   *
   * @return the name of the routing executor
   */
  public String getRouterExecutorName() {
    return routerExecutorName;
  }

}
