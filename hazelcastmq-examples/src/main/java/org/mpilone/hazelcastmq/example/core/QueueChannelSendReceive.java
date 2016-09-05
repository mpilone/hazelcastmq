package org.mpilone.hazelcastmq.example.core;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.*;
import java.util.concurrent.TimeUnit;
import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import static java.lang.String.format;

/**
 * Example of subscribing to a queue and sending a message to the queue.
 */
public class QueueChannelSendReceive extends ExampleApp {

  private final static ILogger log = Logger.getLogger(QueueChannelSendReceive.class);

  public static void main(String[] args) throws Exception {
    QueueChannelSendReceive app = new QueueChannelSendReceive();
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

      long startTime = System.currentTimeMillis();

      log.info("Sending message.");
      channel.send(new GenericMessage<>("Hello World!"));

      log.info("Receiving message.");
      org.mpilone.hazelcastmq.core.Message<?> msg = channel.receive(2,
          TimeUnit.SECONDS);

      long endTime = System.currentTimeMillis();

      log.info(format("Sent and received message '%s' in %d milliseconds.",
          msg.getPayload(), (endTime - startTime)));
    }
    finally {
      hz.shutdown();
    }
  }
}
