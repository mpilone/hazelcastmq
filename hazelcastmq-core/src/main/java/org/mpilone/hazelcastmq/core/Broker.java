package org.mpilone.hazelcastmq.core;

/**
 * <p>
 * The broker that provides access to router and channel contexts. A broker is
 * thread-safe. Normally only a single broker is required for an application but
 * multiple can be created if needed. A broker must be closed when it is no
 * longer needed to release resources. All contexts created by the broker will
 * also be closed when the broker is closed.
 * </p>
 * <p>
 * Refer to the EIP Message Broker for more information:
 * http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageBroker.html
 * </p>
 *
 * @author mpilone
 */
public interface Broker extends AutoCloseable {

  /**
   * Creates a channel context that can be used to create channels and manage
   * transactions.
   *
   * @return a new channel context instance
   */
  ChannelContext createChannelContext();

  /**
   * Creates a router context that can be used to create and manage routes
   * between channels.
   *
   * @return a new router context instance
   */
  RouterContext createRouterContext();

  /**
   * Returns the broker configuration. The configuration cannot be modified
   * after the broker is created. Modifying the configuration will lead to
   * undefined results and possible errors.
   *
   * @return the broker configuration
   */
  BrokerConfig getConfig();

  /**
   * Closes the broker which will automatically close all contexts and channels.
   * This method will block until all resources are stopped and released.
   */
  @Override
  public void close();

}
