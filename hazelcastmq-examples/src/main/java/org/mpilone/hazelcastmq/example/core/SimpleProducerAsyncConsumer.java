package org.mpilone.hazelcastmq.example.core;

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
public class SimpleProducerAsyncConsumer implements HazelcastMQMessageListener {

  private final Logger log = LoggerFactory.getLogger(getClass());

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQMessageListener#onMessage(org.mpilone
   * .hazelcastmq.core.HazelcastMQMessage)
   */
  @Override
  public void onMessage(HazelcastMQMessage msg) {
    log.info("Received message: " + msg.getContentAsString());
  }

  public static void main(String[] args) throws Exception {
    new SimpleProducerAsyncConsumer();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public SimpleProducerAsyncConsumer() throws Exception {

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

      HazelcastMQContext mqProducerContext = mqInstance.createContext();
      HazelcastMQContext mqConsumerContext = mqInstance.createContext();

      HazelcastMQProducer mqProducer = mqProducerContext.createProducer();

      HazelcastMQConsumer mqConsumer = mqConsumerContext
          .createConsumer(destination);
      mqConsumer.setMessageListener(this);

      HazelcastMQMessage msg = new HazelcastMQMessage();
      msg.setContentAsString("Hello World!");

      log.info("Sending message.");
      mqProducer.send(destination, msg);

      // Wait to receive the message.
      Thread.sleep(1000);

      mqConsumer.close();
      mqProducerContext.stop();
      mqProducerContext.close();
      mqConsumerContext.stop();
      mqConsumerContext.close();
      mqInstance.shutdown();
    }
    finally {
      hz.getLifecycleService().shutdown();
    }
  }
}
