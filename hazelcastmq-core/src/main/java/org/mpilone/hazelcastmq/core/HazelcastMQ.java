package org.mpilone.hazelcastmq.core;

/**
 * Entry point into HazelcastMQ. This factory creates new HazelcastMQ broker
 * instances * based on a given configuration or a default configuration will be
 * constructed.
 * 
 * @author mpilone
 */
public class HazelcastMQ {

  /**
   * Returns a new broker that will use a default   * configuration.
   * 
   * @return a new broker instance
   */
  public static Broker newBroker() {
    return newBroker(null);
  }

  /**
   * Returns a new broker using the given configuration. If   * the configuration is null, a default configuration will be used.
   * 
   * @param config
   *          the configuration for the instance
   * @return the new broker instance
   */
  public static Broker newBroker(
      BrokerConfig config) {

    if (config == null) {
      config = new BrokerConfig();
    }

    return new DefaultBroker(config);
  }
}
