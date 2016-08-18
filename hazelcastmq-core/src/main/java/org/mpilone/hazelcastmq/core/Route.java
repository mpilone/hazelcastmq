
package org.mpilone.hazelcastmq.core;

import java.util.*;

/**
 *
 * @author mpilone
 */
public class Route {
  private final DataStructureKey channelKey;
  private final Set<String> routingKeys;

  public Route(DataStructureKey channelKey) {
    this(channelKey, Collections.emptySet());
  }

  public Route(DataStructureKey channelKey, Collection<String> routingKeys) {
    this.channelKey = channelKey;
    this.routingKeys = new HashSet<>(routingKeys);
  }

  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  public Collection<String> getRoutingKeys() {
    return Collections.unmodifiableSet(routingKeys);
  }

}
