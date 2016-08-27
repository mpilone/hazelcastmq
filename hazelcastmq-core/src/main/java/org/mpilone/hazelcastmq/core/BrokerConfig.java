package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * The configuration for the broker.
 *
 * @author mpilone
 */
public class BrokerConfig {

  // Possible future config options:
  // - multiple thread router executor
  // - nack destination channel
  // - in-flight timeout
  //
  //
  private HazelcastInstance hazelcastInstance;
  private MessageConverter messageConverter = new NoOpMessageConverter();

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


}
