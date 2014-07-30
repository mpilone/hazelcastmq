package org.mpilone.hazelcastmq.example.core;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.*;

/**
 * Example of subscribing to a queue and sending a large number of message
 * through the queue.
 */
public class ProducerConsumerLargeSend extends ExampleApp {

  /**
   * The number of messages to push through the queue.
   */
  private static final int MESSAGE_COUNT = 2000;

  private final ILogger log = Logger.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    ProducerConsumerLargeSend app = new ProducerConsumerLargeSend();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

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
      HazelcastMQContext mqContext = mqInstance.createContext();

      HazelcastMQProducer mqProducer = mqContext.createProducer();

      HazelcastMQConsumer mqConsumer = mqContext.createConsumer(destination);

     

      long startTime = System.currentTimeMillis();

      log.info("Sending messages.");
      for (int i = 0; i < MESSAGE_COUNT; ++i) {
        HazelcastMQMessage msg = new HazelcastMQMessage();
        msg.setBody("Hello World " + i);
        mqProducer.send(destination, msg);
      }

      log.info("Receiving messages.");
      int receivedCount = 0;
      HazelcastMQMessage msg;
      do {
        msg = mqConsumer.receive(2, TimeUnit.SECONDS);
        receivedCount += (msg == null ? 0 : 1);
      }
      while (msg != null);

      long endTime = System.currentTimeMillis();

      log.info(format("Sent %d and received %d messages in %d milliseconds "
          + "(serially).", MESSAGE_COUNT, receivedCount, (endTime - startTime)));

      mqConsumer.close();
      mqContext.stop();
      mqContext.close();
      mqInstance.shutdown();
    }
    finally {
      hz.shutdown();
    }
  }
 
}
