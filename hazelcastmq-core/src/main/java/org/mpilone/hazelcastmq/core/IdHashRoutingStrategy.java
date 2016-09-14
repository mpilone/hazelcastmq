
package org.mpilone.hazelcastmq.core;

import java.util.*;

import static java.util.Arrays.asList;

/**
 * A routing strategy that selects a single target route using the hash code of
 * the message ID. This will result in roughly round-robin like behavior but
 * equal distribution is not guaranteed. If true round-robin behavior is
 * required, see the {@link RoundRobinRoutingStrategy}.
 *
 * @author mpilone
 */
public class IdHashRoutingStrategy implements RoutingStrategy {
  private static final long serialVersionUID = 1L;

  @Override
  public Collection<DataStructureKey> routeMessage(
      Message<?> msg, Collection<Route> routes) {

    if (routes.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the hash of the message ID.
    int target = msg.getHeaders().getId().hashCode() % routes.size();

    // Sort the routes.
    DataStructureKey channelKey = routes.stream().sorted(Comparator.comparing(
        Route::getChannelKey)).skip(target).findFirst().get().getChannelKey();

    return asList(channelKey);
  }

}
