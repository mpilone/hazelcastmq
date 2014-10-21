package org.mpilone.hazelcastmq.example.core;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a message to a queue and then waiting for a reply using
 * the {@link HazelcastMQConfig.ContextDispatchStrategy#REACTOR}.
 */
public class ProducerRequestReplyReactor extends ExampleApp {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerRequestReplyReactor().runExample();
  }
  
  @Override
  protected void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hz);
      mqConfig.setExecutor(Executors.newSingleThreadExecutor());
      mqConfig.setContextDispatchStrategy(
          HazelcastMQConfig.ContextDispatchStrategy.REACTOR);

      String requestDest = "/queue/example.dest";

      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      try (HazelcastMQContext mqContext = mqInstance.createContext()) {

        // Create a temporary reply-to queue.
        String replyDest = mqContext.createTemporaryQueue();

        HazelcastMQMessage msg = new HazelcastMQMessage();
        msg.setBody("Do some work and get back to me.");
        msg.setReplyTo(replyDest);
        msg.setCorrelationId(UUID.randomUUID().toString());
        msg.setContentType("text/plain");

        // Send a message with a reply-to header.
        HazelcastMQProducer mqProducer = mqContext.createProducer(requestDest);
        mqProducer.send(msg);

        // Create a consumer on the request destination. Normally this would
        // be done in a different node.
        try (HazelcastMQConsumer mqConsumer =
            mqContext.createConsumer(requestDest)) {

          msg = mqConsumer.receive(2, TimeUnit.SECONDS);
          String rd = msg.getReplyTo();
          String corrId = msg.getCorrelationId();

          log.info("Got message [{}]. Sending reply to [{}].", msg.
              getBodyAsString(), rd);

          // Build the reply message.
          msg = new HazelcastMQMessage();
          msg.setBody("Work is done. You're welcome.");
          msg.setContentType("text/plain");
          msg.setCorrelationId(corrId);

          // Create a producer on the reply destination.
          mqProducer = mqContext.createProducer(rd);
          mqProducer.send(msg);
        }

        // Create a consumer on the reply destination.
        try (HazelcastMQConsumer mqConsumer =
            mqContext.createConsumer(replyDest)) {

          msg = mqConsumer.receive(2, TimeUnit.SECONDS);

          log.info("Got reply message [{}].", msg.getBodyAsString());
        }

        mqContext.stop();
      }
      mqInstance.shutdown();
      mqConfig.getExecutor().shutdown();
    }
    finally {
      hz.getLifecycleService().shutdown();
    }
  }
}
