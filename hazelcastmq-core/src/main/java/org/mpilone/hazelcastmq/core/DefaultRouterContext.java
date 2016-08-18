
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.*;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * @author mpilone
 */
public class DefaultRouterContext implements RouterContext {

  private final static String ROUTER_DATA_MAP_NAME = "hzmq.routerdata";

  private final DefaultBroker broker;
  private final HazelcastInstance hazelcastInstance;
  private final List<Router> routers;
  private final Object routerMutex;

  private volatile boolean closed;

  public DefaultRouterContext(DefaultBroker broker) {
    this.broker = broker;
    this.hazelcastInstance = broker.getConfig().getHazelcastInstance();
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
    broker.remove(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  void remove(Router router) {
    synchronized (routerMutex) {
      routers.remove(router);
    }
  }

  @Override
  public Router createRouter(DataStructureKey channelKey) {

    synchronized (routerMutex) {
      requireNotClosed();

      
      DefaultRouter router = new DefaultRouter(this, channelKey);
      routers.add(router);

      return router;
    }
  }

  IMap<DataStructureKey, RouterData> getRouterDataMap() {
    return hazelcastInstance.getMap(ROUTER_DATA_MAP_NAME);
  }

  DefaultBroker getBroker() {
    return broker;
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

  static class RouterData implements Serializable {

    private final DataStructureKey channelKey;
    private final RoutingStrategy routingStrategy;
    private final Collection<Route> routes;

    public RouterData(DataStructureKey channelKey) {
      this(channelKey, new FanOutRoutingStrategy(), Collections.emptyList());
    }

    public RouterData(DataStructureKey channelKey,
        RoutingStrategy routingStrategy, Collection<Route> routes) {
      this.channelKey = channelKey;
      this.routingStrategy = routingStrategy;
      this.routes = Collections.unmodifiableCollection(routes);
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
