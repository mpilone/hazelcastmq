package org.mpilone.hazelcastmq.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * The default and primary implementation of the broker.
 *
 * @author mpilone
 */
public class DefaultBroker implements Broker {

  private final BrokerConfig config;
  private final Object contextMutex;
  private final List<ChannelContext> channelContexts;
  private final List<RouterContext> routerContexts;
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
  }

  @Override
  public ChannelContext createChannelContext() {
    requireNotClosed();

    synchronized (contextMutex) {
      ChannelContext context = new DefaultChannelContext(this);
      channelContexts.add(context);
      return context;
    }
  }

  @Override
  public RouterContext createRouterContext() {

    synchronized (contextMutex) {
      RouterContext context = new DefaultRouterContext(this);
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

}
