package org.mpilone.hazelcastmq.example.stomp;

import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;

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
 * This example uses a Stomper STOMP server to accept a Stompee client
 * connection. The client then sends and receives a STOMP frame. The Stomper
 * server is backed by the HazelcastMQ {@link ConnectionFactory} which is backed
 * by a local Hazelcast instance.
 * 
 * @author mpilone
 * 
 */
public class StompToStompOneWay {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new StompToStompOneWay();
  }

  public StompToStompOneWay() throws Exception {

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

      // Subscribe to a queue.
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompClient.write(frame);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!").build();
      stompClient.write(frame);

      // Now consume that message.
      frame = stompClient.receive(3, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive expected frame!");

      log.info("Got frame: "
          + new String(frame.getBody(), StompConstants.UTF_8));

      // Shutdown the client.
      stompClient.disconnect();

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
