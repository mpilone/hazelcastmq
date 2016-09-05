package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.BaseQueue;
import com.hazelcast.core.HazelcastInstance;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * The default and primary implementation of the broker.
 *
 * @author mpilone
 */
class DefaultBroker implements Broker {

  private final BrokerConfig config;
  private final Object contextMutex;
  private final List<ChannelContext> channelContexts;
  private final List<RouterContext> routerContexts;
  private final HazelcastInstance hazelcastInstance;
  private final String routeExecutorRegistrationId;
  private final String entryExpirerRegistrationId;
  private final String inflightMapRegistrationId;
  private final String inflightQueueRegistrationId;
  private final DataStructureContext dataStructureContext;
  private final TrackingParent<Broker> parent;
  private final String name;
  private final List<Stoppable> stoppableListeners;

  private volatile boolean closed;

  /**
   * Constructs the broker with the given configuration.
   *
   * @param config the broker configuration
   */
  public DefaultBroker(TrackingParent<Broker> parent, String name,
      BrokerConfig config) {
    this.config = config;
    this.name = name;
    this.parent = parent;

    this.contextMutex = new Object();
    this.channelContexts = new LinkedList<>();
    this.routerContexts = new LinkedList<>();
    this.hazelcastInstance = config.getHazelcastInstance();
    this.dataStructureContext = new HazelcastDataStructureContext();
    this.stoppableListeners = new ArrayList<>(5);

    // Listen for message sends and cleanup sent map.
    final MessageSentCleanupHandler cleanupHandler
        = new MessageSentCleanupHandler(name, config);
    this.entryExpirerRegistrationId = MessageSentAdapter.
        getMapToListen(dataStructureContext).addEntryListener(
            cleanupHandler, false);
    stoppableListeners.add(cleanupHandler);

    // Listen for message sends and perform routing.
    final MessageSentRoutingHandler routingHandler
        = new MessageSentRoutingHandler(name, config);
    this.routeExecutorRegistrationId = MessageSentAdapter.getMapToListen(
        dataStructureContext).
        addEntryListener(routingHandler, false);
    stoppableListeners.add(routingHandler);

    // Listen for inflight messages and ack/nacks and apply them.
    final MessageAckInflightApplyHandler inflightHandler
        = new MessageAckInflightApplyHandler(name, config);
    this.inflightMapRegistrationId = MessageAckInflightAdapter
        .getMapToListen(dataStructureContext).addEntryListener(inflightHandler,
            false);
    this.inflightQueueRegistrationId = MessageAckInflightAdapter
        .getQueueToListen(dataStructureContext).addItemListener(inflightHandler,
            false);
    stoppableListeners.add(inflightHandler);
  }

  @Override
  public ChannelContext createChannelContext() {
    requireNotClosed();

    synchronized (contextMutex) {
      ChannelContext context = new DefaultChannelContext(this::remove, config);
      channelContexts.add(context);
      return context;
    }
  }

  @Override
  public RouterContext createRouterContext() {

    synchronized (contextMutex) {
      RouterContext context = new DefaultRouterContext(this::remove, config);
      routerContexts.add(context);
      return context;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public BrokerConfig getConfig() {
    return config;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    // Stop listening for message sent events.
    MessageSentAdapter.getMapToListen(dataStructureContext).
        removeEntryListener(entryExpirerRegistrationId);
    MessageSentAdapter.getMapToListen(dataStructureContext).
        removeEntryListener(routeExecutorRegistrationId);
    MessageAckInflightAdapter.getMapToListen(dataStructureContext)
        .removeEntryListener(inflightMapRegistrationId);
    MessageAckInflightAdapter.getQueueToListen(dataStructureContext)
        .removeItemListener(inflightQueueRegistrationId);

    try {
      for (Stoppable s : stoppableListeners) {
        s.stop();
      }
      stoppableListeners.clear();
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    synchronized (contextMutex) {
      // Clone the list so there is no concurrent modification exception.
      new ArrayList<>(channelContexts).stream().forEach(ChannelContext::close);
      channelContexts.clear();

      new ArrayList<>(routerContexts).stream().forEach(RouterContext::close);
      routerContexts.clear();
    }

    parent.remove(this);
  }

  /**
   * Removes the given context from the list of open contexts.
   *
   * @param context the context that was closed
   */
  void remove(ChannelContext context) {
    synchronized (contextMutex) {
      channelContexts.remove(context);
    }
  }

  void remove(RouterContext context) {
    synchronized (contextMutex) {
      routerContexts.remove(context);
    }
  }

  /**
   * Checks if the context is closed and throws an exception if it is.
   *
   * @throws HazelcastMQException if the context is closed
   */
  private void requireNotClosed() throws HazelcastMQException {
    if (closed) {
      throw new HazelcastMQException("Broker is closed.");
    }
  }

  private class HazelcastDataStructureContext implements DataStructureContext {

    @Override
    public <E> BaseQueue<E> getQueue(String name, boolean joinTransaction) {
      return hazelcastInstance.getQueue(name);
    }

    @Override
    public <K, V> BaseMap<K, V> getMap(String name, boolean joinTransaction) {
      return hazelcastInstance.getMap(name);
    }
  }

}
