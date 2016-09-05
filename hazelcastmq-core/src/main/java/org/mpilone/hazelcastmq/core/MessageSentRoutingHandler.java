
package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.Serializable;

import static java.lang.String.format;

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

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      MessageSentRoutingHandler.class);

  private final String brokerName;
  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;

  /**
   * Constructs the executor.
   *
   * @param config the broker configuration
   */
  public MessageSentRoutingHandler(String brokerName,
      BrokerConfig config) {
    this.config = config;
    this.brokerName = brokerName;
    this.hazelcastInstance = config.getHazelcastInstance();
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    final IExecutorService executor = hazelcastInstance.getExecutorService(
        config.getRouterExecutorName());
    final Member member = hazelcastInstance.getCluster().getLocalMember();

    executor.executeOnMember(new RouteMessagesTask(channelKey,
        brokerName), member);
  }

  private static class RouteMessagesTask implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerName;
    private final DataStructureKey channelKey;

    public RouteMessagesTask(DataStructureKey channelKey, String brokerName) {
      this.brokerName = brokerName;
      this.channelKey = channelKey;
    }

    @Override
    public void run() {

      Broker broker = HazelcastMQ.getBrokerByName(brokerName);

      if (broker == null) {
        // Log and return.
        log.warning(format("Unable to find broker %s to route messages "
            + "for channel key %s.", brokerName, channelKey));

        return;
      }

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
