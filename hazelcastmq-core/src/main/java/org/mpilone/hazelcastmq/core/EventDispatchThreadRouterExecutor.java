
package org.mpilone.hazelcastmq.core;

/**
 * A router executor that uses the item event dispatch thread to perform the
 * routing. This is the most direct routing but it will block event dispatch on
 * the message sent map so it does not support parallel routing and it may block
 * channel read-ready notifications.
 *
 * @author mpilone
 */
class EventDispatchThreadRouterExecutor extends MessageSentMapAdapter implements
    RouterExecutor {

  private final BrokerConfig config;

  /**
   * Constructs the executor.
   *
   * @param config the broker configuration
   */
  public EventDispatchThreadRouterExecutor(BrokerConfig config) {
    this.config = config;
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    try (RouterContext routerContext = new DefaultRouterContext(child -> {
    }, config)) {

      // Don't create a router if it doesn't already exist otherwise we'll
      // always route messages to an empty list of targets even if the user
      // didn't explicitly create and configure a router.
      if (routerContext.containsRouterChannelKey(channelKey)) {
        try (Router router = routerContext.createRouter(channelKey)) {
          router.routeMessages();
        }
      }
    }
  }

}
