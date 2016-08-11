package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.*;

/**
 * Example of subscribing to a queue and sending a message to the queue. The
 * producer will be running in a full Hz data node while the consumer will be
 * running as a client only node.
 */
public class QueueChannelHzClient extends ExampleApp {

  private final static ILogger log = Logger.
      getLogger(QueueChannelHzClient.class);

  public static void main(String[] args) throws Exception {
    QueueChannelHzClient app = new QueueChannelHzClient();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().setPort(6071);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcastMQ broker.
    BrokerConfig brokerConfig = new BrokerConfig(hz);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Create a separate consumer node that will connect as a client only (no data) node.
      Thread consumerNode = new Thread(new ConsumerNode());
      consumerNode.start();

      try (ChannelContext mqContext = broker.createChannelContext();
          Channel channel = mqContext.createChannel(new DataStructureKey(
                  "example.dest",
                  QueueService.SERVICE_NAME))) {

        org.mpilone.hazelcastmq.core.Message<String> msg = MessageBuilder.
            withPayload("Hello World!").build();

      log.info("Sending message.");
        channel.send(msg);

      // Wait for the consumer (which should be almost instant
      // if not done already).
      consumerNode.join();
      }
    }
    finally {
      hz.shutdown();
    }
  }

  private class ConsumerNode implements Runnable {

    @Override
    public void run() {

      // Create a Hazelcast instance.
      ClientConfig config = new ClientConfig();
      config.getNetworkConfig().addAddress("localhost:6071");
      HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

      // Create the HazelcastMQ broker.
      BrokerConfig brokerConfig = new BrokerConfig(hz);
      try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      try (ChannelContext mqContext = broker.createChannelContext();
          Channel channel = mqContext.createChannel(new DataStructureKey(
                  "example.dest",
                  QueueService.SERVICE_NAME))) {

          org.mpilone.hazelcastmq.core.Message<?> msg = channel.receive(10,
              TimeUnit.SECONDS);
            if (msg != null) {
              log.info("Got message: " + msg.getPayload());
            }
            else {
              log.severe("Failed to get message.");
            }
          }

        }
      finally {
        hz.shutdown();
      }
    }

  }
}
