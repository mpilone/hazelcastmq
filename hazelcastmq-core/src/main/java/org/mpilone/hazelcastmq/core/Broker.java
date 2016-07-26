package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public interface Broker extends AutoCloseable {

  ChannelContext createChannelContext();

  RouterContext createRouterContext();

  BrokerConfig getConfig();

  @Override
  public void close();

}
