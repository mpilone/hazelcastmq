package org.mpilone.hazelcastmq.core;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 *
 * @author mpilone
 */
public class FanOutRoutingStrategy implements RoutingStrategy {

  @Override
  public Collection<DataStructureKey> routeMessage(
      Message<?> msg, Collection<Route> routes) {

    return routes.stream().map(Route::getChannelKey).collect(Collectors.toSet());

  }

}
