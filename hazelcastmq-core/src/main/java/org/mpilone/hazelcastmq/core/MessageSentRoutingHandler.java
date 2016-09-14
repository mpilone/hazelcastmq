
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.concurrent.Executor;


/**
 * Routes messages whenever a message is sent. The execution is performed by an
 * {@link Executor} instance which can select the appropriate threading strategy
 * and node.
 *
 * @author mpilone
 */
class MessageSentRoutingHandler extends MessageSentAdapter {

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

  /**
   * The task that executes the routing using the router for the given source
   * channel. If there is no router configured for the channel, this method does
   * nothing.
   */
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
