
package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
public class AckAndNack extends ExampleApp {

  private final static ILogger log = Logger.getLogger(
      AckAndNack.class);

  public static void main(String[] args) throws Exception {
    AckAndNack app = new AckAndNack();
    app.runExample();
  }

  /**
   * Constructs the example.
   *
   * @throws Exception if the example fails
   */
  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    BrokerConfig brokerConfig = new BrokerConfig(hz);

    // Create the broker.
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig);
        ChannelContext channelContext = broker.createChannelContext();
        Channel channel = channelContext.createChannel(new DataStructureKey(
                "example.dest", QueueService.SERVICE_NAME))) {

      channelContext.setAckMode(AckMode.CLIENT_INDIVIDUAL);

      // Send some messages.
      log.info("Sending messages.");
      IntStream.range(0, 5).forEach(i -> {
        channel.send(MessageBuilder.withPayload("Payload " + i).build());
      });

      // Receive two messages.
      Message<?> msg1 = channel.receive();
      log.info("Got message: " + msg1.getPayload());

      Message<?> msg2 = channel.receive();
      log.info("Got message: " + msg2.getPayload());

      // Ack the two messages.
      channelContext.ack(msg1.getHeaders().getId(), msg2.getHeaders().getId());

      // Receive two more messages.
      msg1 = channel.receive();
      log.info("Got message: " + msg1.getPayload());

      msg2 = channel.receive();
      log.info("Got message: " + msg2.getPayload());

      // Ack one, nack the other.
      channelContext.ack(msg1.getHeaders().getId());
      channelContext.nack(msg2.getHeaders().getId());

      // Receive two more messages. There should be two more because we
      // nacked one message so it should have been resent.
      msg1 = channel.receive();
      log.info("Got message: " + msg1.getPayload());
      channelContext.ack(msg1.getHeaders().getId());

      msg2 = channel.receive(2, TimeUnit.SECONDS);
      if (msg2 == null) {
        log.warning("Nacked message was not available in the "
            + "original channel as expected.");
      }
      else {
        log.info("Got message: " + msg2.getPayload());
        channelContext.ack(msg2.getHeaders().getId());
      }

      // There should be no more messages.
      msg1 = channel.receive(1, TimeUnit.SECONDS);
      if (msg1 != null) {
        log.warning("Got a message when none was expected!");
      }
    }
    finally {
      hz.shutdown();
    }
  }
}
