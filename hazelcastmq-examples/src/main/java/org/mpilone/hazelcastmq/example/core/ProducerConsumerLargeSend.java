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
public class ProducerConsumerLargeSend {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerLargeSend();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerLargeSend() throws Exception {

    String destination = "/queue/example.dest";

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

      HazelcastMQConsumer mqConsumer = mqContext.createConsumer(destination);

      HazelcastMQMessage msg = new HazelcastMQMessage();
      msg.setContentAsString("Hello World!");

      long startTime = System.currentTimeMillis();

      log.info("Sending messages.");
      for (int i = 0; i < 500; ++i) {
        mqProducer.send(destination, msg);
      }

      log.info("Receiving messages.");
      int receivedCount = 0;
      do {
        msg = mqConsumer.receive(2, TimeUnit.SECONDS);
        receivedCount += (msg == null ? 0 : 1);
      }
      while (msg != null);

      long endTime = System.currentTimeMillis();

      log.info("Sent and received {} messages in {} milliseconds.",
          receivedCount, (endTime - startTime));

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
