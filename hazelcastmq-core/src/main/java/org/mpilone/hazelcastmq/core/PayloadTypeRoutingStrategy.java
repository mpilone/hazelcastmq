
package org.mpilone.hazelcastmq.core;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * A routing strategy that will route to any channel that has a routing key
 * matching the class name of the message payload. For example, if the payload
 * was a String, any route with a routing key of {@code java.lang.String} would
 * match and be included in the set of destination channels. There can be
 * multiple destination channels if multiple routes contain a matching routing
 * key.
 *
 * @author mpilone
 */
public class PayloadTypeRoutingStrategy implements RoutingStrategy {

  @Override
  public Collection<DataStructureKey> routeMessage(
      Message<?> msg, Collection<Route> routes) {

    final Object payload = msg.getPayload();

    Collection<DataStructureKey> targets;
    if (payload != null && !routes.isEmpty()) {

      // Get the payload class name.
      final String payloadClass = payload.getClass().getName();

      // Filter the routes to find ones with a matching routing key.
      targets = routes.stream().filter(route -> {
        return route.getRoutingKeys().contains(payloadClass);
      }).map(Route::getChannelKey).collect(Collectors.toSet());

    }
    else {
      // No payload or routes. Return an empty set.
      targets = Collections.emptySet();
    }

    return targets;
  }

}
