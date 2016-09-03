package org.mpilone.hazelcastmq.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point into HazelcastMQ. This factory creates new HazelcastMQ broker
 * instances based on a given configuration or a default configuration will be
 * constructed.
 *
 * @author mpilone
 */
public class HazelcastMQ {

  private final static AtomicInteger COUNT = new AtomicInteger();
  private final static ConcurrentHashMap<String, Broker> BROKER_MAP =
      new ConcurrentHashMap<>(5);

  /**
   * Returns a new broker that will use a default configuration.
   *
   * @return a new broker instance
   */
  public static Broker newBroker() {
    return newBroker(null);
  }

  /**
   * Returns the broker with the given name.
   *
   * @param brokerName the name of the broker to match
   *
   * @return the broker instance or null if there is no matching broker
   */
  public static Broker getBrokerByName(String brokerName) {
    return BROKER_MAP.get(brokerName);
  }

  /**
   * Returns a new broker using the given configuration. If the configuration is
   * null, a default configuration will be used.
   *
   * @param config the configuration for the instance
   *
   * @return the new broker instance
   */
  public static Broker newBroker(BrokerConfig config) {

    if (config == null) {
      config = new BrokerConfig();
    }

    final String name = "hzmq.broker." + COUNT.incrementAndGet();
    final TrackingParent<Broker> parent = broker -> {
      BROKER_MAP.remove(broker.getName());
    };

    Broker b = new DefaultBroker(parent, name, config);
    BROKER_MAP.put(name, b);
    return b;
  }
}
