package org.mpilone.hazelcastmq.core;

/**
 * An interface that indicates that the implementor requires a {@link Broker}
 * instance before executing. This class is normally used in combination with
 * the {@link DistributedBrokerExecutor}.
 *
 * @author mpilone
 */
interface BrokerAware {

  /**
   * Sets the broker on the instance.
   *
   * @param broker the broker
   */
  void setBroker(Broker broker);

}
