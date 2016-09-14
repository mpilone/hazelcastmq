
package org.mpilone.hazelcastmq.core;

import java.util.*;

import static java.util.Arrays.asList;

/**
 * A routing strategy that selects a single target route using a simple
 * round-robin algorithm. This strategy is stateful in order to keep track of
 * the last route that was dispatched to, therefore if pure round-robin isn't
 * required, the {@link IdHashRoutingStrategy} may be a little faster and give
 * similar results.
 *
 * @author mpilone
 */
public class RoundRobinRoutingStrategy implements RoutingStrategy,
    StatefulRoutingStrategy {
  private static final long serialVersionUID = 1L;

  private long counter = 1;

  @Override
  public Collection<DataStructureKey> routeMessage(
      Message<?> msg, Collection<Route> routes) {

    if (routes.isEmpty()) {
      return Collections.emptyList();
    }

    // Determine the target based on the counter.
    int target = (int) (counter % routes.size());

    // Increment the counter.
    counter++;

    // Sort the routes.
    DataStructureKey channelKey = routes.stream().sorted(Comparator.comparing(
        Route::getChannelKey)).skip(target).findFirst().get().getChannelKey();

    return asList(channelKey);
  }

}
