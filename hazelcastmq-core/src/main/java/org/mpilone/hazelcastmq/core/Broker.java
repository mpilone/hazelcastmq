package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public interface Broker {

  ChannelContext createChannelContext();

  RouterContext createRouterContext();

}
