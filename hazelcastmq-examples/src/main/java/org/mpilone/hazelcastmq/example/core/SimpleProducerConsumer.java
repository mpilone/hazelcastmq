package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of subscribing to a queue and sending a message to the queue.
 */
public class SimpleProducerConsumer {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new SimpleProducerConsumer();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public SimpleProducerConsumer() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hz);

      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);
      HazelcastMQContext mqContext = mqInstance.createContext();

      HazelcastMQProducer mqProducer = mqContext.createProducer();

      HazelcastMQConsumer mqConsumer = mqContext
          .createConsumer("/queue/example.dest");

      HazelcastMQMessage msg = new HazelcastMQMessage();
      msg.setContentAsString("Hello World!");

      long startTime = System.currentTimeMillis();

      log.info("Sending message.");
      mqProducer.send("/queue/example.dest", msg);

      log.info("Receiving message.");
      msg = mqConsumer.receive(2, TimeUnit.SECONDS);

      long endTime = System.currentTimeMillis();

      log.info("Received message '{}' in {} milliseconds.",
          msg.getContentAsString(), (endTime - startTime));

      mqConsumer.close();
      mqContext.stop();
      mqContext.close();
      mqInstance.shutdown();
    }
    finally {
      hz.getLifecycleService().shutdown();
    }
  }
}
