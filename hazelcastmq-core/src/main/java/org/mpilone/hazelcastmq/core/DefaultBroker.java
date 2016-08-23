package org.mpilone.hazelcastmq.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.BaseQueue;
import com.hazelcast.core.HazelcastInstance;

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
  private final DataStructureContext dataStructureContext;

  private volatile boolean closed;

  /**
   * Constructs the broker with the given configuration.
   *
   * @param config the broker configuration
   */
  public DefaultBroker(BrokerConfig config) {
    this.config = config;

    this.contextMutex = new Object();
    this.channelContexts = new LinkedList<>();
    this.routerContexts = new LinkedList<>();
    this.hazelcastInstance = config.getHazelcastInstance();
    this.dataStructureContext = new HazelcastDataStructureContext();

    this.entryExpirerRegistrationId = MessageSentMapAdapter.
        getMapToListen(dataStructureContext).addEntryListener(
            new MessageSentMapEntryExpirer(config), false);
    this.routeExecutorRegistrationId = MessageSentMapAdapter.getMapToListen(
        dataStructureContext).
        addEntryListener(new EntryProcessorRouterExecutor(config), false);
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
    MessageSentMapAdapter.getMapToListen(dataStructureContext).
        removeEntryListener(entryExpirerRegistrationId);
    MessageSentMapAdapter.getMapToListen(dataStructureContext).
        removeEntryListener(routeExecutorRegistrationId);

    synchronized (contextMutex) {
      // Clone the list so there is no concurrent modification exception.
      new ArrayList<>(channelContexts).stream().forEach(ChannelContext::close);
      channelContexts.clear();

      new ArrayList<>(routerContexts).stream().forEach(RouterContext::close);
      routerContexts.clear();
    }
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
