
package org.mpilone.hazelcastmq.stomp.server;

/**
 *
 * @author mpilone
 */
public class HazelcastMQStomp {
  /**
   * Returns a new {@link HazelcastMQStompInstance} that will use a default
   * configuration.
   *
   * @return a new HazelcastMQ instance
   */
  public static HazelcastMQStompInstance newHazelcastMQStompInstance() {
    return newHazelcastMQStompInstance(null);
  }

  /**
   * Returns a new {@link HazelcastMQStompInstance} using the given
   * configuration. If the configuration is null, a default configuration will
   * be used.
   *
   * @param config the configuration for the instance
   *
   * @return the new HazelcastMQ instance
   */
  public static HazelcastMQStompInstance newHazelcastMQStompInstance(
      HazelcastMQStompConfig config) {

    if (config == null) {
      config = new HazelcastMQStompConfig();
    }

    return new DefaultHazelcastMQStompInstance(config);
  }
}
