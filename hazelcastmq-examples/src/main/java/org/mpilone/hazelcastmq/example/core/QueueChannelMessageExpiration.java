package org.mpilone.hazelcastmq.example.core;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request message to a local Hazelcast instance while
 * acting as both the producer and consumer of the message. The message will
 * have an expiration time that will pass before the consumer gets to the
 * message.
 */
public class QueueChannelMessageExpiration extends ExampleApp {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new QueueChannelMessageExpiration().runExample();
  }

  @Override
  public void start() throws Exception {

    final DataStructureKey destination = DataStructureKey.fromString(
        "/queue/example.test1");

    // Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

      // Create the HazelcastMQ broker.
    // Create a context and channel.
    BrokerConfig brokerConfig = new BrokerConfig(hazelcast);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig);
        ChannelContext context = broker.createChannelContext();
        Channel channel = context.createChannel(destination)) {

      // Create a message producer.
      log.info("Sending message.");
      Message<String> msg = MessageBuilder.withPayload(
          "I only have a few seconds to live!").setHeader(
              MessageHeaders.EXPIRATION, Instant.now().plus(3,
                  ChronoUnit.SECONDS).toEpochMilli()).build();

      channel.send(msg);

      // Simulate a long wait before the consumer gets the message
      log.info("Waiting for message to expire.");
      Thread.sleep(5000);

      // Create a message consumer.
      log.info("Receiving message.");
      msg = (Message<String>) channel.receive(0, TimeUnit.SECONDS);
      Assert.isNull(msg, "Received message that should have expired.");
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }
}
