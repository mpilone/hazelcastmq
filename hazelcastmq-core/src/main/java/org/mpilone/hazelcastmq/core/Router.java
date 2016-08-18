package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.Collection;

/**
 *
 * @author mpilone
 */
public interface Router extends Serializable, AutoCloseable {

  static final String DEFAULT_ROUTING_KEY = "";

  @Override
  public void close();

  boolean isClosed();

  void addRoute(DataStructureKey targetKey, String... routingKeys);

  void removeRoute(DataStructureKey targetKey, String... routingKeys);

  DataStructureKey getChannelKey();

  Collection<Route> getRoutes();

  RoutingStrategy getRoutingStrategy();

  void setRoutingStrategy(RoutingStrategy strategy);

}
