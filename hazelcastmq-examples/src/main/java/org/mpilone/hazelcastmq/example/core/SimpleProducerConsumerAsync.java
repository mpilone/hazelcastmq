package org.mpilone.hazelcastmq.example.core;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of subscribing to a queue and sending a message to the queue.
 */
public class SimpleProducerConsumerAsync extends ExampleApp implements
    HazelcastMQMessageListener {

  private final static ILogger log = Logger.getLogger(
      SimpleProducerConsumerAsync.class);

  private final CountDownLatch receiveLatch = new CountDownLatch(1);

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQMessageListener#onMessage(org.mpilone
   * .hazelcastmq.core.HazelcastMQMessage)
   */
  @Override
  public void onMessage(HazelcastMQMessage msg) {
    log.info("Received message: " + msg.getBodyAsString());
    receiveLatch.countDown();
  }

  public static void main(String[] args) throws Exception {
    SimpleProducerConsumerAsync app = new SimpleProducerConsumerAsync();
    app.runExample();
  }

  /**
   * Constructs the example.
   * 
   * @throws Exception if the example fails
   */
  @Override
  public void start() throws Exception {

    String destination = "/queue/example.dest";

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
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
      msg.setBody("Hello World!");

      log.info("Sending message.");
      mqProducer.send(destination, msg);

      // Wait to receive the message.
      receiveLatch.await(10, TimeUnit.SECONDS);

      mqConsumer.close();
      mqProducerContext.close();
      mqConsumerContext.close();
      mqInstance.shutdown();
    }
    finally {
      hz.shutdown();
    }
  }
}
