package org.mpilone.hazelcastmq.example;

import javax.jms.*;

import org.mpilone.hazelcastmq.HazelcastMQConfig;
import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.mpilone.hazelcastmq.stomp.Frame;
import org.mpilone.hazelcastmq.stomp.FrameBuilder;
import org.mpilone.hazelcastmq.stompee.HazelcastMQStompee;
import org.mpilone.hazelcastmq.stompee.HazelcastMQStompeeConfig;
import org.mpilone.hazelcastmq.stomper.HazelcastMQStomper;
import org.mpilone.hazelcastmq.stomper.HazelcastMQStomperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * This example uses a Stomper STOMP server to accept a Stompee client
 * connection. The client then sends a STOMP frame. A JMS consumer then consumes
 * the message. The Stomper server is backed by the HazelcastMQ
 * {@link ConnectionFactory} which is backed by a local Hazelcast instance. This
 * example shows how STOMP clients connected to the Stompee server, backed by
 * HazelcastMQ, can easily interoperate with JMS producers and consumers.
 * 
 * @author mpilone
 * 
 */
public class StomperStompeeJmsOneWay {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new StomperStompeeJmsOneWay();
  }

  public StomperStompeeJmsOneWay() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      HazelcastMQConnectionFactory connectionFactory = new HazelcastMQConnectionFactory(
          hazelcast, mqConfig);

      // Create a Stomper server.
      HazelcastMQStomperConfig stomperConfig = new HazelcastMQStomperConfig(
          connectionFactory);
      HazelcastMQStomper stomper = new HazelcastMQStomper(stomperConfig);

      log.info("Stomper is now listening on port: " + stomperConfig.getPort());

      // Create a Stompee client.
      HazelcastMQStompeeConfig stompeeConfig = new HazelcastMQStompeeConfig(
          "localhost", stomperConfig.getPort());
      HazelcastMQStompee stompee = new HazelcastMQStompee(stompeeConfig);

      // Send a message to a queue.
      Frame frame = FrameBuilder.send("/queue/demo.test", "Hello World!")
          .build();
      stompee.send(frame);

      // Now create a JMS consumer to consume that message.
      Connection connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue("demo.test");
      MessageConsumer consumer = session.createConsumer(destination);

      Message msg = consumer.receive(120000);
      Assert.notNull(msg, "Did not get required message.");
      Assert.isTrue(msg instanceof TextMessage,
          "Did not get correct message type.");

      log.info("Got message: " + ((TextMessage) msg).getText());

      // Shutdown the JMS consumer
      consumer.close();
      session.close();
      connection.close();

      // Shutdown the client.
      stompee.shutdown();

      // Shutdown the server.
      log.info("Shutting down Stomper.");
      stomper.shutdown();
      stomperConfig.getExecutor().shutdown();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }

  }
}
