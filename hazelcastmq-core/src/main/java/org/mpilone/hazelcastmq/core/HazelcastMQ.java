package org.mpilone.hazelcastmq.core;

/**
 * Entry point into HazelcastMQ. This factory creates new HazelcastMQ instances
 * based on a given configuration or a default configuration will be
 * constructed.
 * 
 * @author mpilone
 */
public class HazelcastMQ {

  /**
   * Returns a new {@link HazelcastMQInstance} that will use a default
   * configuration.
   * 
   * @return a new HazelcastMQ instance
   */
  public static HazelcastMQInstance newHazelcastMQInstance() {
    return newHazelcastMQInstance(null);
  }

  /**
   * Returns a new {@link HazelcastMQInstance} using the given configuration. If
   * the configuration is null, a default configuration will be used.
   * 
   * @param config
   *          the configuration for the instance
   * @return the new HazelcastMQ instance
   */
  public static HazelcastMQInstance newHazelcastMQInstance(
      HazelcastMQConfig config) {

    if (config == null) {
      config = new HazelcastMQConfig();
    }

    return new DefaultHazelcastMQInstance(config);
  }
}
