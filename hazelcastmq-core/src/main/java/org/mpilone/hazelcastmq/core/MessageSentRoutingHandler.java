
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.concurrent.Executor;


/**
 * A router executor that uses the item event dispatch thread to perform the
 * routing. This is the most direct routing but it will block event dispatch on
 * the message sent map so it does not support parallel routing and it may block
 * channel read-ready notifications.
 *
 * @author mpilone
 */
class MessageSentRoutingHandler extends MessageSentAdapter implements
    RouterExecutor {

  private final Executor executor;

  /**
   * Constructs the executor.
   *
   * @param config the broker configuration
   */
  public MessageSentRoutingHandler(Executor executor) {
    this.executor = executor;
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    executor.execute(new RouteMessagesTask(channelKey));
  }

  private static class RouteMessagesTask implements Runnable, Serializable,
      BrokerAware {

    private static final long serialVersionUID = 1L;

    private final DataStructureKey channelKey;

    private Broker broker;

    public RouteMessagesTask(DataStructureKey channelKey) {
      this.channelKey = channelKey;
    }

    @Override
    public void setBroker(Broker broker) {
      this.broker = broker;
    }

    @Override
    public void run() {

      try (RouterContext routerContext = broker.createRouterContext()) {

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
}
