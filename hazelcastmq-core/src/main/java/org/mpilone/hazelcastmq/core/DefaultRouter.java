package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.EntryProcessor;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.mpilone.hazelcastmq.core.DefaultRouterContext.RouterData;

/**
 * Default implementation of a router that uses a router data map to track the
 * router's state across the cluster.
 *
 * @author mpilone
 */
class DefaultRouter implements Router {

  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;
  private final TrackingParent<Router> parent;
  private final DataStructureKey channelKey;

  private volatile boolean closed;

  /**
   * Constructs the router.
   *
   * @param channelKey the source channel key
   * @param parent the parent context that created the router
   * @param config the broker configuration for access to the Hazelcast instance
   */
  DefaultRouter(DataStructureKey channelKey, TrackingParent<Router> parent,
      BrokerConfig config) {
    this.config = config;
    this.channelKey = channelKey;
    this.hazelcastInstance = config.getHazelcastInstance();
    this.parent = parent;

    // Make sure the router data exists for this router in the data map.
    getRouterDataMap().putIfAbsent(channelKey, new RouterData(channelKey));
  }

  @Override
  public void close() {
    closed = true;

    parent.remove(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private IMap<DataStructureKey, RouterData> getRouterDataMap() {
    return hazelcastInstance.getMap(DefaultRouterContext.ROUTER_DATA_MAP_NAME);
  }

  @Override
  public void addRoute(DataStructureKey targetKey, String... routingKeys) {
    requireNotClosed();

    executeOnKeyLocally(new AddRouteProcessor(targetKey, routingKeys));
  }

  @Override
  public void removeRoute(DataStructureKey targetKey, String... routingKeys) {
    requireNotClosed();

    executeOnKeyLocally(new RemoveRouteProcessor(targetKey, routingKeys));
  }

  @Override
  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  @Override
  public Collection<Route> getRoutes() {
    requireNotClosed();

    // The route list in RouterData is already unmodifiable.
    return getRouterDataMap().get(channelKey).getRoutes();
  }

  @Override
  public RoutingStrategy getRoutingStrategy() {
    requireNotClosed();

    return getRouterDataMap().get(channelKey).getRoutingStrategy();
  }

  @Override
  public void setRoutingStrategy(RoutingStrategy strategy) {
    requireNotClosed();

    executeOnKeyLocally(new SetRoutingStrategyProcessor(strategy));
  }

  /**
   * Checks if the router is closed and throws an exception if it is.
   *
   * @throws HazelcastMQException if the context is closed
   */
  private void requireNotClosed() throws HazelcastMQException {
    if (closed) {
      throw new HazelcastMQException("Router is closed.");
    }
  }

  /**
   * Executes the given processor on the router data map after locking the
   * source channel key. This method is similar to {@link IMap#executeOnKey(java.lang.Object, com.hazelcast.map.EntryProcessor)
   * } except that the processor is always executed in the current thread.
   *
   * @param processor the processor to execute
   */
  private void executeOnKeyLocally(
      EntryProcessor<DataStructureKey, RouterData> processor) {

    final IMap<DataStructureKey, RouterData> routerDataMap = getRouterDataMap();

    // Lock the channel key to avoid concurrent modification to the
    // strategy and routes during routing.
    routerDataMap.lock(channelKey);
    try {
      Map.Entry entry = new WriteThroughEntry(channelKey, routerDataMap);
      processor.process(entry);
    }
    finally {
      routerDataMap.unlock(channelKey);
    }

  }

  /**
   * Routes all messages from the source channel to the target channels using
   * the routing strategy configured in the router.
   */
  @Override
  public void routeMessages() {
    requireNotClosed();

    executeOnKeyLocally(new RouteMessagesProcessor(config));
  }

  /**
   * Routes all messages from the source channel to the target channels using
   * the routing strategy configured in the router. This processor must be
   * executed   * in the router lock to avoid race conditions on the router data.
   */
  private static class RouteMessagesProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {
    private static final long serialVersionUID = 1L;

    private final BrokerConfig config;

    public RouteMessagesProcessor(BrokerConfig config) {
      super(false);
      this.config = config;
    }

    /**
     * Routes all messages from the source channel to the target channels using
     * the routing strategy configured in the router.
     *
     * @param sourceChannelKey the key for the channel that serves as the source
     * of messages
     * @param routes the routes that define the target channel keys and routing
     * keys
     * @param strategy the routing strategy to apply to each message
     */
  private void routeMessages(final DataStructureKey sourceChannelKey,
      final Collection<Route> routes, final RoutingStrategy strategy) {

    // Create a channel context to access input and output channels.
    try (DefaultChannelContext channelContext = new DefaultChannelContext(
        child -> {
        }, config)) {

      // Create the input channel to read from.
      try (Channel sourceChannel = channelContext.
          createChannel(sourceChannelKey)) {

        // As long as we have messages, keep routing.
        Message<?> msg;
        while ((msg = sourceChannel.receive(0, TimeUnit.SECONDS)) != null) {

          // Use the strategy to route the message and send it to each target channel.
          final Message<?> _msg = msg;
          strategy.routeMessage(_msg, routes).stream().forEach(targetKey -> {

            try (Channel targetChannel = channelContext.createChannel(targetKey)) {
              // Send to the target channel.
              // TODO: we should probably log if a send fails or have a
              // configuration options for sending to the DLQ.
              targetChannel.send(_msg, 0, TimeUnit.SECONDS);
            }
          });
        }
      }
    }
    }

    @Override
    public Object process(Map.Entry<DataStructureKey, RouterData> entry) {

      final RouterData routerData = entry.getValue();

      // Get the strategy and possible output routes.
      final RoutingStrategy strategy = routerData.getRoutingStrategy();
      final Collection<Route> routes = routerData.getRoutes();
      final DataStructureKey sourceChannelKey = routerData.getChannelKey();

      routeMessages(sourceChannelKey, routes, strategy);

      if (strategy instanceof StatefulRoutingStrategy) {
        // Put the router data back in the map so the stateful strategy
        // is properly saved/persisted.
        entry.setValue(routerData);

        return routerData;
      } else {
        return null;
      }
    }
  }

  /**
   * Entry processor that adds a target route to this router.
   */
  private static class AddRouteProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {

    private static final long serialVersionUID = 1L;

    private final DataStructureKey targetKey;
    private final String[] routingKeys;

    /**
     * Constructs the processor that will add the target channel key with the
     * optional routing keys. If no routing keys are specified, the default
     * routing key will be used.
     *
     * @param targetKey the target channel key
     * @param routingKeys the routing keys or empty or null for the default
     */
    public AddRouteProcessor(DataStructureKey targetKey, String[] routingKeys) {
      super(true);

      this.targetKey = targetKey;
      this.routingKeys = routingKeys;
    }

    @Override
    public Object process(Map.Entry<DataStructureKey, RouterData> entry) {

      final RouterData data = entry.getValue();

      if (data == null) {
        return null;
      }

      final Map<DataStructureKey, Route> routeMap = data.getRoutes().stream().
          collect(Collectors.toMap(Route::getChannelKey, r -> r));
      final boolean routeExists = routeMap.containsKey(targetKey);
      final Set<String> newRoutingKeys = routeExists ? new HashSet(routeMap.get(
          targetKey).getRoutingKeys()) : new HashSet<>();

      if (routingKeys == null || routingKeys.length == 0) {
        // Add the default routing key.
        newRoutingKeys.add(DEFAULT_ROUTING_KEY);
      }
      else {
        // Add all the routing keys given.
        newRoutingKeys.addAll(Arrays.asList(routingKeys));
      }

      // Put the entry into the map.
      routeMap.put(targetKey, new Route(targetKey, newRoutingKeys));

      RouterData newData = new RouterData(data.getChannelKey(), data.
          getRoutingStrategy(), routeMap.values());
      entry.setValue(newData);

      return newData;

    }
  }

  /**
   * Entry processor that removes a target route to this router.
   */
  private static class RemoveRouteProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {

    private static final long serialVersionUID = 1L;

    private final DataStructureKey targetKey;
    private final String[] routingKeys;

    /**
     * Constructs the processor that will remove the target channel key with the
     * optional routing keys. If no routing keys are specified, the target will
     * be completely removed.
     *
     * @param targetKey the target channel key
     * @param routingKeys the routing keys or empty or null to remove the target
     * completely
     */
    public RemoveRouteProcessor(DataStructureKey targetKey, String[] routingKeys) {
      super(true);

      this.targetKey = targetKey;
      this.routingKeys = routingKeys;
    }

    @Override
    public Object process(Map.Entry<DataStructureKey, RouterData> entry) {

      final RouterData data = entry.getValue();

      if (data == null) {
        return null;
      }

      final Map<DataStructureKey, Route> routeMap = data.getRoutes().stream().
          collect(Collectors.toMap(Route::getChannelKey, r -> r));
      final boolean routeExists = routeMap.containsKey(targetKey);
      final Set<String> newRoutingKeys = routeExists ? new HashSet(routeMap.get(
          targetKey).getRoutingKeys()) : new HashSet<>();

      if (routingKeys == null || routingKeys.length == 0) {
        // Remove all routing keys.
        newRoutingKeys.clear();
      }
      else {
        // Remove the specific routing keys.
        newRoutingKeys.removeAll(Arrays.asList(routingKeys));
      }

      if (newRoutingKeys.isEmpty()) {
        // Remove the route if all routing keys are gone.
        routeMap.remove(targetKey);
      }
      else {
        // Update the route with the new routing keys.
        routeMap.put(targetKey, new Route(targetKey, newRoutingKeys));
      }

      RouterData newData = new RouterData(data.getChannelKey(), data.
          getRoutingStrategy(), routeMap.values());
      entry.setValue(newData);

      return newData;
    }
  }

  /**
   * Entry processor that sets the routing strategy on an entry.
   */
  private static class SetRoutingStrategyProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {

    private static final long serialVersionUID = 1L;

    private final RoutingStrategy strategy;

    public SetRoutingStrategyProcessor(RoutingStrategy strategy) {
      super(true);

      this.strategy = strategy;
    }

    @Override
    public Object process(Map.Entry<DataStructureKey, RouterData> entry) {
      final RouterData data = entry.getValue();

      if (data == null) {
        return null;
      }

      RouterData newData = new RouterData(data.getChannelKey(), strategy,
          data.getRoutes());
      entry.setValue(newData);

      return newData;
    }
  }

  /**
   * A map entry that reads and writes to a backing map implementation. That is,
   * the get and set value operations delegate to the backing map and do not
   * store the value locally.
   *
   * @param <K> the type of the entry key
   * @param <V> the type of the entry value
   */
  private static class WriteThroughEntry<K, V> implements Map.Entry<K, V> {

    private final K key;
    private final Map<K, V> target;

    /**
     * Constructs the entry that will read and write to the given key in the
     * given map.
     *
     * @param key the entry key
     * @param target the backing target map to read from and write to
     */
    public WriteThroughEntry(K key, Map<K, V> target) {
      this.key = key;
      this.target = target;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return target.get(key);
    }

    @Override
    public V setValue(V value) {
      return target.put(key, value);
    }
  }

}
