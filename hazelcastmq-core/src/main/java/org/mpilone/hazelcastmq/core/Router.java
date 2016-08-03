package org.mpilone.hazelcastmq.core;

import java.util.Collection;

/**
 *
 * @author mpilone
 */
public interface Router {

  void addRoute(DataStructureKey targetKey);

  void removeRoute(DataStructureKey targetKey);

  Collection<DataStructureKey> getRoutes();

  RoutingStrategy getRoutingStrategy();

  void setRoutingStrategy(RoutingStrategy strategy);

}
