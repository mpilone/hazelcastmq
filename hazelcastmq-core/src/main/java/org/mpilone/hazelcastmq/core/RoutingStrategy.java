package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.Collection;

/**
 *
 * @author mpilone
 */
public interface RoutingStrategy extends Serializable {

  Collection<DataStructureKey> routeMessage(Message<?> msg,
      Collection<Route> routes);

}
