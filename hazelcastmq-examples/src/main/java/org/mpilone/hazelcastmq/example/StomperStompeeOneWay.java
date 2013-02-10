package org.mpilone.hazelcastmq.example;

import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;

import org.mpilone.hazelcastmq.HazelcastMQConfig;
import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.mpilone.hazelcastmq.stomp.Frame;
import org.mpilone.hazelcastmq.stomp.FrameBuilder;
import org.mpilone.hazelcastmq.stomp.StompConstants;
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
 * connection. The client then sends and receives a STOMP frame. The Stomper
 * server is backed by the HazelcastMQ {@link ConnectionFactory} which is backed
 * by a local Hazelcast instance.
 * 
 * @author mpilone
 * 
 */
public class StomperStompeeOneWay {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new StomperStompeeOneWay();
  }

  public StomperStompeeOneWay() throws Exception {

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

      // Subscribe to a queue.
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompee.send(frame);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!").build();
      stompee.send(frame);

      // Now consume that message.
      frame = stompee.receive(300, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive expected frame!");

      log.info("Got frame: "
          + new String(frame.getBody(), StompConstants.UTF_8));

      // Shutdown the client.
      stompee.shutdown();

      // Shutdown the server.
      log.info("Shutting down Stomper.");
      stomper.shutdown();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }

  }
}
