package org.mpilone.hazelcastmq.example.stomp;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.stomp.server.HazelcastMQStompServer;
import org.mpilone.hazelcastmq.stomp.server.HazelcastMQStompServerConfig;
import org.mpilone.stomp.Frame;
import org.mpilone.stomp.FrameBuilder;
import org.mpilone.stomp.StompConstants;
import org.mpilone.stomp.client.BasicStompClient;
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
      HazelcastMQStompServerConfig stompConfig = new HazelcastMQStompServerConfig(
          mqInstance);
      HazelcastMQStompServer stompServer = new HazelcastMQStompServer(
          stompConfig);

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      // Create a Stomp client.
      BasicStompClient stompClient = new BasicStompClient();
      stompClient.connect("localhost", stompConfig.getPort());

      // Create a Stomp client to receive.
      BasicStompClient stompClient2 = new BasicStompClient();
      stompClient2.connect("localhost", stompConfig.getPort());

      // Subscribe to a queue.
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompClient2.write(frame);

      // Start a transaction
      final String transactionId = "tx1";
      frame = FrameBuilder.begin(transactionId).build();
      stompClient.write(frame);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!")
          .header(org.mpilone.stomp.Headers.TRANSACTION, transactionId).build();
      stompClient.write(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = stompClient2.receive(2, TimeUnit.SECONDS);
      Assert.isNull(frame, "Received unexpected frame!");

      // Now commit the transaction.
      frame = FrameBuilder.commit(transactionId).build();
      stompClient.write(frame);

      // Now try to consume that message. We shouldn't get anything because the
      // transaction hasn't been committed.
      frame = stompClient2.receive(2, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive unexpected frame!");

      log.info("Got expected frame: "
          + new String(frame.getBody(), StompConstants.UTF_8));

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
