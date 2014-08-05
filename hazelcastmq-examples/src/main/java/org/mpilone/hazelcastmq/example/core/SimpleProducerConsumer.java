package org.mpilone.hazelcastmq.example.core;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.*;

/**
 * Example of subscribing to a queue and sending a message to the queue.
 */
public class SimpleProducerConsumer extends ExampleApp {

  private final static ILogger log = Logger.getLogger(
      SimpleProducerConsumer.class);

  public static void main(String[] args) throws Exception {
    SimpleProducerConsumer app = new SimpleProducerConsumer();
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
      msg.setBody("Hello World!");

      long startTime = System.currentTimeMillis();

      log.info("Sending message.");
      mqProducer.send("/queue/example.dest", msg);

      log.info("Receiving message.");
      msg = mqConsumer.receive(2, TimeUnit.SECONDS);

      long endTime = System.currentTimeMillis();

      log.info(format("Received message '%s' in %d milliseconds.",
          msg.getBodyAsString(), (endTime - startTime)));

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
