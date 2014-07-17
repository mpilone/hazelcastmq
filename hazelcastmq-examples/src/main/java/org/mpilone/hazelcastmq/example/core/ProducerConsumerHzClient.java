package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * Example of subscribing to a queue and sending a message to the queue. The
 * producer will be running in a full Hz data node while the consumer will be
 * running as a client only node.
 */
public class ProducerConsumerHzClient {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    new ProducerConsumerHzClient();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerHzClient() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().setPort(6071);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hz);

      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // Create a separate consumer node that will connect as a client only (no data) node.
      Thread consumerNode = new Thread(new ConsumerNode());
      consumerNode.start();

      HazelcastMQContext mqContext = mqInstance.createContext();

      HazelcastMQProducer mqProducer = mqContext.createProducer();

      HazelcastMQMessage msg = new HazelcastMQMessage();
      msg.setBody("Hello World!");

      log.info("Sending message.");
      mqProducer.send("/queue/example.dest", msg);

      // Wait for the consumer (which should be almost instant
      // if not done already).
      consumerNode.join();

      mqContext.stop();
      mqContext.close();
      mqInstance.shutdown();
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

      try {
        // Setup the connection factory.
        HazelcastMQConfig mqConfig = new HazelcastMQConfig();
        mqConfig.setHazelcastInstance(hz);

        HazelcastMQInstance mqInstance = HazelcastMQ
            .newHazelcastMQInstance(mqConfig);

        try (HazelcastMQContext mqContext = mqInstance.createContext()) {

          try (HazelcastMQConsumer mqConsumer =
              mqContext.createConsumer("/queue/example.dest")) {

            HazelcastMQMessage msg = mqConsumer.receive(10, TimeUnit.SECONDS);
            if (msg != null) {
              System.out.println("Got message: " + msg.getBodyAsString());
            }
            else {
              System.out.println("Failed to get message.");
            }
          }

          mqContext.stop();
        }
        mqInstance.shutdown();
      }
      finally {
        hz.shutdown();
      }
    }

  }
}
