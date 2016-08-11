package org.mpilone.hazelcastmq.example.core;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.*;

/**
 * Example of subscribing to a queue and sending a large number of message
 * through the queue.
 */
public class QueueChannelLargeSend extends ExampleApp {

  /**
   * The number of messages to push through the queue.
   */
  private static final int MESSAGE_COUNT = 20000;

  private final static ILogger log = Logger.getLogger(QueueChannelLargeSend.class);

  public static void main(String[] args) throws Exception {
    QueueChannelLargeSend app = new QueueChannelLargeSend();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    final DataStructureKey destination = DataStructureKey.fromString(
        "/queue/example.dest");

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcastMQ broker.
    BrokerConfig brokerConfig = new BrokerConfig(hz);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Create the context and channel.
      try (ChannelContext mqContext = broker.createChannelContext();
          Channel channel = mqContext.createChannel(destination)) {

      long startTime = System.currentTimeMillis();

      log.info("Sending messages.");
      for (int i = 0; i < MESSAGE_COUNT; ++i) {
        channel.send(MessageBuilder.withPayload("Hello world " + i).build());
      }

      log.info("Receiving messages.");
      int receivedCount = 0;
        org.mpilone.hazelcastmq.core.Message<String> msg;
      do {
        msg = (org.mpilone.hazelcastmq.core.Message<String>) channel.receive(0,
            TimeUnit.SECONDS);
        receivedCount += (msg == null ? 0 : 1);
      }
      while (msg != null);

      long endTime = System.currentTimeMillis();
      long elapsed = endTime - startTime;
      long msgsPerSec = (long) (receivedCount * (1000.0 / elapsed));

      log.info(format("Sent and received %d messages in %d milliseconds "
          + "(in serial) for an average of %d messages per second.",
          MESSAGE_COUNT, elapsed, msgsPerSec));

      }
    }
    finally {
      hz.shutdown();
    }
  }
 
}
