package org.mpilone.hazelcastmq.core;

import java.util.Collection;

/**
 * A router that routes messages from a source channel to target output channels
 * using a routing strategy. Target channels can be added with routing keys that
 * may be used by the routing strategy to select targets dynamically.
 * Modifications to a router are applied immediately across the cluster.
 *
 * @author mpilone
 */
public interface Router extends AutoCloseable {

  /**
   * The default routing key that is automatically applied to a target channel
   * when no specific routing key is specified.
   */
  static final String DEFAULT_ROUTING_KEY = "";

  /**
   * Closes the router and releases resources. Closing a router does not destroy
   * it but simply closes this handle to it. The router will be active until it
   * is destroyed.
   */
  @Override
  public void close();

  /**
   * Returns true if the router is closed and cannot be used anymore.
   *
   * @return true if the router is closed
   */
  boolean isClosed();

  /**
   * Adds a route to a target channel to this router. If a route to the target
   * channel key already exists, the routing keys are added to the existing
   * route. If a route to the target channel key does not already exist, a new
   * route is created with the given routing keys. If the routing keys are empty
   * or null, the default routing key will be used.
   *
   * @param targetKey the target channel key to route to
   * @param routingKeys the routing keys to associate with the route
   */
  void addRoute(DataStructureKey targetKey, String... routingKeys);

  /**
   * Removes a route to the target channel from this router. If routing keys are
   * given, only the routing keys will be removed from the route. If the routing
   * keys are empty or null, the route to the target will be completely removed.
   *
   * @param targetKey the target channel key to route to
   * @param routingKeys the routing keys to disassociate with the route
   */
  void removeRoute(DataStructureKey targetKey, String... routingKeys);

  /**
   * Returns the channel key for the source channel in the route.
   *
   * @return the source channel key
   */
  DataStructureKey getChannelKey();

  /**
   * Returns the target routes configured in this router.
   *
   * @return the target routes or an empty collection
   */
  Collection<Route> getRoutes();

  /**
   * Returns the routing strategy configured in this router. The strategy will
   * be used whenever the router routes messages from the source channel to the
   * target channels.
   *
   * @return the routing strategy
   */
  RoutingStrategy getRoutingStrategy();

  /**
   * Sets the routing strategy configured in this router. The strategy will be
   * used whenever the router routes messages from the source channel to the
   * target channels.
   *
   * @param strategy the routing strategy
   */
  void setRoutingStrategy(RoutingStrategy strategy);

}
