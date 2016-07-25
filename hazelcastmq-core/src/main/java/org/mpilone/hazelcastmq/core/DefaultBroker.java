package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public class DefaultBroker implements Broker {

  private final BrokerConfig config;

  public DefaultBroker(BrokerConfig config) {
    this.config = config;
  }

  @Override
  public ChannelContext createChannelContext() {
    return new DefaultChannelContext(this);
  }

  @Override
  public RouterContext createRouterContext() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  BrokerConfig getConfig() {
    return config;
  }
}
