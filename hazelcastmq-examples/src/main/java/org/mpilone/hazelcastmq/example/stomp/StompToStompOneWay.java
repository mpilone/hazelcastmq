package org.mpilone.hazelcastmq.example.stomp;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.stomp.HazelcastMQStomp;
import org.mpilone.hazelcastmq.stomp.StompAdapter;
import org.mpilone.hazelcastmq.stomp.StompAdapterConfig;
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
public class StompToStompOneWay extends ExampleApp {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      StompToStompOneWay.class);

  public static void main(String[] args) throws Exception {
    new StompToStompOneWay().runExample();
  }

  @Override
  protected void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcaseMQ broker.
    BrokerConfig mqConfig = new BrokerConfig();
    mqConfig.setHazelcastInstance(hazelcast);

    try (Broker broker = HazelcastMQ.newBroker(mqConfig)) {

      // Create a Stomp server.
      StompAdapterConfig stompConfig = new StompAdapterConfig(broker);
      try (StompAdapter stompServer = HazelcastMQStomp.newStompAdapter(
          stompConfig)) {

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
      }
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.shutdown();
    }

  }
}
