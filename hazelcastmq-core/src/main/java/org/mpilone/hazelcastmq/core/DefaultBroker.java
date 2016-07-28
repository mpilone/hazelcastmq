package org.mpilone.hazelcastmq.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mpilone
 */
public class DefaultBroker implements Broker {

  private final BrokerConfig config;
  private final Object contextMutex;
  private final List<ChannelContext> channelContexts;
  private final List<RouterContext> routerContexts;
  private volatile boolean closed;

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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
