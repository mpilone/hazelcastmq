package org.mpilone.hazelcastmq.example.stomp;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.stomp.server.HazelcastMQStomp;
import org.mpilone.hazelcastmq.stomp.server.HazelcastMQStompConfig;
import org.mpilone.hazelcastmq.stomp.server.HazelcastMQStompInstance;
import org.mpilone.yeti.Frame;
import org.mpilone.yeti.FrameBuilder;
import org.mpilone.yeti.client.StompClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * This example uses a stomp-server to accept a stomp-client connection. The
 * client then sends and receives a STOMP frame. The stomp-server is backed by
 * the {@link HazelcastMQInstance} which is backed by a local Hazelcast
 * instance.
 * 
 * @author mpilone
 */
public class StompToStompOneWayTransaction {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    new StompToStompOneWayTransaction();
  }

  public StompToStompOneWayTransaction() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // Create the HazelcaseMQ instance.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);
      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // Create a Stomp server.
      HazelcastMQStompConfig stompConfig = new HazelcastMQStompConfig(
          mqInstance);
      HazelcastMQStompInstance stompServer = HazelcastMQStomp.
          newHazelcastMQStompInstance(stompConfig);

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      // Create a Stomp client.
      StompClient stompClient = new StompClient("localhost", stompConfig.
          getPort());
      stompClient.connect();

      // Create a Stomp client to poll.
      StompClient stompClient2 = new StompClient("localhost", stompConfig.
          getPort());
      stompClient2.connect();

      // Subscribe to a queue.
      StompClient.QueuingFrameListener msgListener =
          new StompClient.QueuingFrameListener();
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompClient2.subscribe(frame, msgListener);

      // Start a transaction
      final String transactionId = "tx1";
      frame = FrameBuilder.begin(transactionId).build();
      stompClient.begin(frame);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!")
          .header(org.mpilone.yeti.Headers.TRANSACTION, transactionId).build();
      stompClient.send(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = msgListener.poll(2, TimeUnit.SECONDS);
      Assert.isNull(frame, "Received unexpected frame!");

      // Now commit the transaction.
      frame = FrameBuilder.commit(transactionId).build();
      stompClient.commit(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = msgListener.poll(2, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive unexpected frame!");

      log.info("Got expected frame: " + frame.getBodyAsString());

      // Shutdown the client.
      stompClient.disconnect();
      stompClient2.disconnect();

      // Shutdown the server.
      log.info("Shutting down STOMP server.");
      stompServer.shutdown();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }

  }
}
