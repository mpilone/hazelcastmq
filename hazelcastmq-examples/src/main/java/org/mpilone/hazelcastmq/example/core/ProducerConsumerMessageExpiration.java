package org.mpilone.hazelcastmq.example.core;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
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
public class ProducerConsumerMessageExpiration {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerMessageExpiration();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerMessageExpiration() throws JMSException,
      InterruptedException {

    String destination = "/queue/example.test1";

    // Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // HazelcastMQ instance.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);
      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // Create a connection, session, and destinations.
      HazelcastMQContext mqContext = mqInstance.createContext();
      mqContext.start();

      // Create a message producer.
      log.info("Sending message.");
      HazelcastMQProducer mqProducer = mqContext.createProducer();
      mqProducer.setTimeToLive(3000);
      mqProducer.send(destination, "I only have a few seconds to live!");

      // Simulate a long wait before the consumer gets the message
      log.info("Waiting for message to expire.");
      Thread.sleep(5000);

      // Create a message consumer.
      log.info("Receiving message.");
      HazelcastMQConsumer mqConsumer = mqContext.createConsumer(destination);
      byte[] msgBody = mqConsumer.receiveBodyNoWait();
      Assert.isNull(msgBody, "Received message that should have expired.");
      mqConsumer.close();

      // Cleanup
      mqInstance.shutdown();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }
}
