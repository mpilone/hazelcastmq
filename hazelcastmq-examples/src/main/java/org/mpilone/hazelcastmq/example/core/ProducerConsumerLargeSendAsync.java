package org.mpilone.hazelcastmq.example.core;


import static java.lang.String.format;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of subscribing to a queue and sending a large number of messages to
 * the queue.
 */
public class ProducerConsumerLargeSendAsync extends ExampleApp implements
    HazelcastMQMessageListener {

  private final static ILogger log = Logger.getLogger(
      ProducerConsumerLargeSendAsync.class);

  private final static int MESSAGE_COUNT = 10000;

  private final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);

  @Override
  public void onMessage(HazelcastMQMessage msg) {
    receiveLatch.countDown();
  }

  public static void main(String[] args) throws Exception {
    ProducerConsumerLargeSendAsync app = new ProducerConsumerLargeSendAsync();
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

      log.info("Sending messages.");
      long startTime = System.currentTimeMillis();

      for (int i = 0; i < MESSAGE_COUNT; ++i) {
        HazelcastMQMessage msg = new HazelcastMQMessage();
        msg.setBody("Hello World!");
        mqProducer.send(destination, msg);
      }

      log.info("Receiving all messages.");
      receiveLatch.await(10, TimeUnit.SECONDS);

      long endTime = System.currentTimeMillis();
      long elapsed = endTime - startTime;
      long msgsPerSec = (long) (MESSAGE_COUNT * (1000.0 / elapsed));

      log.info(format("Sent and received %d messages in %d milliseconds "
          + "(in parallel) for an average of %d messages per second.",
          MESSAGE_COUNT, elapsed, msgsPerSec
      ));

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
