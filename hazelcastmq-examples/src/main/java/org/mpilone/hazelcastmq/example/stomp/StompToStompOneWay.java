package org.mpilone.hazelcastmq.example.stomp;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.stomp.server.*;
import org.mpilone.yeti.Frame;
import org.mpilone.yeti.FrameBuilder;
import org.mpilone.yeti.client.StompClient;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * This example uses a HazelcastMQ STOMP server to accept a Yeti STOMP client
 * connection. The client then sends and receives a STOMP frame. The server is
 * backed by HazelcastMQ which is backed by a local Hazelcast instance.
  * 
 * @author mpilone
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
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

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

      // Subscribe to a queue.
      StompClient.QueuingFrameListener msgListener =
          new StompClient.QueuingFrameListener();
      Frame frame = FrameBuilder.subscribe("/queue/demo.test", "1").build();
      stompClient.subscribe(frame, msgListener);

      // Send a message on that queue.
      frame = FrameBuilder.send("/queue/demo.test", "Hello World!").build();
      stompClient.send(frame);

      // Now consume that message.
      frame = msgListener.poll(3, TimeUnit.SECONDS);
      Assert.notNull(frame, "Did not receive expected frame!");

      log.info("Got frame: " + frame.getBodyAsString());

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
