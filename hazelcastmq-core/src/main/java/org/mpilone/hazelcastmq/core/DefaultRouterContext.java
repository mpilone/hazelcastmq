
package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.Serializable;
import java.util.*;

/**
 * Default implementation of the router context.
 *
 * @author mpilone
 */
class DefaultRouterContext implements RouterContext, TrackingParent<Router> {

  final static String ROUTER_DATA_MAP_NAME = "hzmq.routerdata";

  private final TrackingParent<RouterContext> parent;
  private final HazelcastInstance hazelcastInstance;
  private final List<Router> routers;
  private final Object routerMutex;
  private final BrokerConfig config;

  private volatile boolean closed;

  /**
   * Constructs the context.
   *
   * @param parent the parent to notify when closing
   * @param config the broker configuration
   */
  public DefaultRouterContext(TrackingParent<RouterContext> parent,
      BrokerConfig config) {
    this.parent = parent;
    this.config = config;
    this.hazelcastInstance = config.getHazelcastInstance();
    this.routers = new LinkedList<>();
    this.routerMutex = new Object();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    // Close any open routers.
    synchronized (routerMutex) {
      new ArrayList<>(routers).stream().forEach(Router::close);
      routers.clear();
    }

    // Remove ourself from the broker.
    parent.remove(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void remove(Router router) {
    synchronized (routerMutex) {
      routers.remove(router);
    }
  }

  @Override
  public Router createRouter(DataStructureKey channelKey) {

    synchronized (routerMutex) {
      requireNotClosed();

      
      DefaultRouter router = new DefaultRouter(channelKey, this, config);
      routers.add(router);

      return router;
    }
  }

  /**
   * Returns the map that contains the source channel key to router data for all
   * configured routers.
   *
   * @return the router data map
   */
  private IMap<DataStructureKey, RouterData> getRouterDataMap() {
    return hazelcastInstance.getMap(ROUTER_DATA_MAP_NAME);
  }

  @Override
  public boolean containsRouterChannelKey(DataStructureKey channelKey) {
    return getRouterDataMap().containsKey(channelKey);
  }

  @Override
  public Set<DataStructureKey> routerChannelKeySet() {
    return Collections.unmodifiableSet(getRouterDataMap().keySet());
  }

  @Override
  public boolean destroyRouter(DataStructureKey channelKey) {
    return getRouterDataMap().remove(channelKey) != null;
  }

  /**
   * Checks if the context is closed and throws an exception if it is.
   *
   * @throws HazelcastMQException if the context is closed
   */
  private void requireNotClosed() throws HazelcastMQException {
    if (closed) {
      throw new HazelcastMQException("Context is closed.");
    }
  }

  /**
   * The raw router data that is persisted in the Hazelcast cluster. The router
   * data is used to back router instances which can modify the data safely.
   */
  static class RouterData implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataStructureKey channelKey;
    private final RoutingStrategy routingStrategy;
    private final Collection<Route> routes;

    /**
     * Constructs the router data that defaults to using a
     * {@link FanOutRoutingStrategy} and empty list of target routes.
     *
     * @param channelKey the source channel key
     */
    public RouterData(DataStructureKey channelKey) {
      this(channelKey, new FanOutRoutingStrategy(), Collections.emptyList());
    }

    /**
     * Constructs the router data.
     *
     * @param channelKey the source channel key
     * @param routingStrategy the routing strategy
     * @param routes the target routes
     */
    public RouterData(DataStructureKey channelKey,
        RoutingStrategy routingStrategy, Collection<Route> routes) {
      this.channelKey = channelKey;
      this.routingStrategy = routingStrategy;
      this.routes = Collections.unmodifiableCollection(new LinkedList<>(routes));
    }

    public DataStructureKey getChannelKey() {
      return channelKey;
    }

    public Collection<Route> getRoutes() {
      return routes;
    }

    public RoutingStrategy getRoutingStrategy() {
      return routingStrategy;
    }
  }

}
