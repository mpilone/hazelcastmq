package org.mpilone.hazelcastmq.core;

import java.util.*;
import java.util.stream.Collectors;

import org.mpilone.hazelcastmq.core.DefaultRouterContext.RouterData;

import com.hazelcast.map.AbstractEntryProcessor;

/**
 *
 * @author mpilone
 */
public class DefaultRouter implements Router {

  private final DefaultRouterContext context;
  private final DataStructureKey channelKey;

  private volatile boolean closed;

  DefaultRouter(DefaultRouterContext context, DataStructureKey channelKey) {
    this.context = context;
    this.channelKey = channelKey;

    // Make sure the router data exists for this router in the data map.
    context.getRouterDataMap().putIfAbsent(channelKey,
        new RouterData(channelKey));
  }

  @Override
  public void close() {
    closed = true;

    context.remove(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void addRoute(DataStructureKey targetKey, String... routingKeys) {
    requireNotClosed();

    context.getRouterDataMap().executeOnKey(channelKey, new AddRouteProcessor(
        targetKey, routingKeys));
  }

  @Override
  public void removeRoute(DataStructureKey targetKey, String... routingKeys) {
    requireNotClosed();

    context.getRouterDataMap().executeOnKey(channelKey,
        new RemoveRouteProcessor(targetKey, routingKeys));
  }

  @Override
  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  @Override
  public Collection<Route> getRoutes() {
    requireNotClosed();

    // The route list in RouterData is already unmodifiable.
    return context.getRouterDataMap().get(channelKey).getRoutes();
  }

  @Override
  public RoutingStrategy getRoutingStrategy() {
    requireNotClosed();

    return context.getRouterDataMap().get(channelKey).getRoutingStrategy();
  }

  @Override
  public void setRoutingStrategy(RoutingStrategy strategy) {
    requireNotClosed();

    context.getRouterDataMap().executeOnKey(channelKey,
        new SetRoutingStrategyProcessor(strategy));
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

  private static class AddRouteProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {

    private final DataStructureKey targetKey;
    private final String[] routingKeys;

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

  private static class RemoveRouteProcessor extends AbstractEntryProcessor<DataStructureKey, RouterData> {

    private final DataStructureKey targetKey;
    private final String[] routingKeys;

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

  private static class SetRoutingStrategyProcessor extends
      AbstractEntryProcessor<DataStructureKey, RouterData> {

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

}
