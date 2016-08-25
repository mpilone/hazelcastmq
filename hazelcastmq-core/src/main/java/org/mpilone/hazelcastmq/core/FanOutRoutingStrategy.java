package org.mpilone.hazelcastmq.core;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A routing strategy that sends all messages to all target channels. The
 * behavior is similar to a traditional publish/subscribe topic where all
 * configured target routes get the same message.
 *
 * @author mpilone
 */
public class FanOutRoutingStrategy implements RoutingStrategy {
  private static final long serialVersionUID = 1L;

  @Override
  public Collection<DataStructureKey> routeMessage(
      Message<?> msg, Collection<Route> routes) {

    return routes.stream().map(Route::getChannelKey).collect(Collectors.toSet());

  }

}
