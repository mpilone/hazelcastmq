
package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.*;

/**
 * A route to a target channel. A route is defined by a target channel key and
 * routing keys that may be used by the routing strategy to select the target
 * channel when routing messages. All routes must have at least one routing key
 * but it may be the default (empty) key.
 *
 * @author mpilone
 */
public class Route implements Serializable {

  private static final long serialVersionUID = 1L;

  private final DataStructureKey channelKey;
  private final Set<String> routingKeys;

  /**
   * Constructs the route. At least 1 routing key is required. The routing keys
   * will be copied into a new set internally.
   *
   * @param channelKey the target channel key
   * @param routingKeys the routing keys
   */
  public Route(DataStructureKey channelKey, Collection<String> routingKeys) {
    Objects.requireNonNull(channelKey, "channelKey may not be null.");
    Objects.requireNonNull(routingKeys, "routingKeys may not be null.");

    if (routingKeys.isEmpty()) {
      throw new IllegalArgumentException(format("At least 1 routing key "
          + "is required on route to channel %s.", channelKey));
    }

    this.channelKey = channelKey;
    this.routingKeys = new HashSet<>(routingKeys);
  }

  /**
   * Returns the target channel key.
   *
   * @return the target channel key
   */
  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  /**
   * Returns the routing keys. At least one routing key will be defined.
   *
   * @return the routing key
   */
  public Collection<String> getRoutingKeys() {
    return Collections.unmodifiableSet(routingKeys);
  }

}
