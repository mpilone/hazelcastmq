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
public class StomperStompeeOneWayTransaction {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new StomperStompeeOneWayTransaction();
  }

  public StomperStompeeOneWayTransaction() throws Exception {

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

      // Create a Stompee client to send.
      HazelcastMQStompeeConfig stompeeConfig = new HazelcastMQStompeeConfig(
          "localhost", stomperConfig.getPort());
      HazelcastMQStompee stompee = new HazelcastMQStompee(stompeeConfig);

      // Create a Stompee client to receive.
      stompeeConfig = new HazelcastMQStompeeConfig("localhost",
          stomperConfig.getPort());
      HazelcastMQStompee stompee2 = new HazelcastMQStompee(stompeeConfig);

      // Subscribe to a queue.
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompee2.send(frame);

      // Start a transaction
      final String transactionId = "tx1";
      frame = FrameBuilder.begin(transactionId).build();
      stompee.send(frame);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!")
          .headerTransaction(transactionId).build();
      stompee.send(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = stompee2.receive(2, TimeUnit.SECONDS);
      Assert.isNull(frame, "Did receive unexpected frame!");

      // Now commit the transaction.
      frame = FrameBuilder.commit(transactionId).build();
      stompee.send(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = stompee2.receive(2, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive unexpected frame!");

      log.info("Got frame: "
          + new String(frame.getBody(), StompConstants.UTF_8));

      // Shutdown the client.
      stompee.shutdown();
      stompee2.shutdown();

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
