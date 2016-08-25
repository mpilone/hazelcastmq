package org.mpilone.hazelcastmq.example.core;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

/**
 * Example of using a round-robin routing strategy to route messages from a
 * single source channel to multiple target channels.
 *
 * @author mpilone
 */
public class RouterRoundRobin extends ExampleApp {

  private final static ILogger log = Logger.getLogger(
      RouterRoundRobin.class);

  public static void main(String[] args) throws Exception {
    RouterRoundRobin app = new RouterRoundRobin();
    app.runExample();
  }

  /**
   * Constructs the example.
   *
   * @throws Exception if the example fails
   */
  @Override
  public void start() throws Exception {

    final DataStructureKey sourceChannelKey = new DataStructureKey(
        "example.source", QueueService.SERVICE_NAME);
    final DataStructureKey target1ChannelKey = new DataStructureKey(
        "example.target1", QueueService.SERVICE_NAME);
    final DataStructureKey target2ChannelKey = new DataStructureKey(
        "example.target2", QueueService.SERVICE_NAME);

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    BrokerConfig brokerConfig = new BrokerConfig(hz);

    // Create the broker.
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Configure the route.
      try (RouterContext context = broker.createRouterContext();
          Router router = context.createRouter(sourceChannelKey)) {

        router.addRoute(target1ChannelKey);
        router.addRoute(target2ChannelKey);
        router.setRoutingStrategy(new RoundRobinRoutingStrategy());
      }

      // Send a messages to the source.
      log.info("Sending messages to the source channel.");
      try (ChannelContext channelContext = broker.createChannelContext();
          Channel channel = channelContext.createChannel(sourceChannelKey)) {

        IntStream.range(0, 10).forEach(i -> {
          channel.send(MessageBuilder.withPayload("Message " + (i + 1)).build());
        });
      }

      try (ChannelContext channelContext = broker.createChannelContext();
          Channel channel1 = channelContext.createChannel(target1ChannelKey);
          Channel channel2 = channelContext.createChannel(target2ChannelKey)) {

        Message<?> msg;

        // Read the messages from the first channel.
        log.info("Receiving messages on target channel 1.");
        while ((msg = channel1.receive(2, TimeUnit.SECONDS)) != null) {
          log.info("Got message: " + msg.getPayload());
        }

        // Read the messages from the first channel.
        log.info("Receiving messages on target channel 2.");
        while ((msg = channel2.receive(2, TimeUnit.SECONDS)) != null) {
          log.info("Got message: " + msg.getPayload());
        }
      }
    }
    finally {
      hz.shutdown();
    }
  }

}
