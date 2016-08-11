package org.mpilone.hazelcastmq.stomp;

/**
 * Factory for creating {@link StompAdapter} instances.
 *
 * @author mpilone
 */
public class HazelcastMQStomp {

  /**
   * Returns a new {@link StompAdapter} that will use a default * configuration.
   *
   * @return a new STOMP adapter instance
   */
  public static StompAdapter newStompAdapter() {
    return newStompAdapter(null);
  }

  /**
   * Returns a new {@link StompAdapter} using the given configuration. If the
   * configuration is null, a default configuration will be used.
   *
   * @param config the configuration for the instance
   *
   * @return the new STOMP adapter instance
   */
  public static StompAdapter newStompAdapter(StompAdapterConfig config) {

    if (config == null) {
      config = new StompAdapterConfig();
    }

    return new DefaultStompAdapter(config);
  }
}
