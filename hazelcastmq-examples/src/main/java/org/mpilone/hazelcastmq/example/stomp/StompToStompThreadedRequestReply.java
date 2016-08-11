package org.mpilone.hazelcastmq.example.stomp;

import org.mpilone.hazelcastmq.stomp.StompAdapterConfig;
import org.mpilone.hazelcastmq.stomp.StompAdapter;
import org.mpilone.hazelcastmq.stomp.HazelcastMQStomp;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.yeti.*;
import org.mpilone.yeti.client.StompClient;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * This example uses a HazelcastMQ STOMP adapter to accept a Yeti STOMP client
 * connection simulating a frontend and a STOMP client connection from another
 * thread simulating a backend. The client then sends a bunch of request frames
 * and waits for replies. The server is backed by HazelcastMQ which is backed by
 * a local Hazelcast instance.
  * 
 * @author mpilone
 */
public class StompToStompThreadedRequestReply extends ExampleApp {

  /**
   * The log for this class.
   */
  private final static Logger log  = LoggerFactory.getLogger(
      StompToStompThreadedRequestReply.class);

  private static final int STOMP_PORT = 8035;
  private static final String REQUEST_QUEUE = "/queue/do.some.work";
  private static final String REPLY_QUEUE = "/queue/do.some.work.replies";

  public static void main(String[] args) throws Exception {
    new StompToStompThreadedRequestReply().runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcaseMQ instance.
    BrokerConfig brokerConfig = new BrokerConfig(hazelcast);

    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Create a Stomp server.
      StompAdapterConfig stompConfig = new StompAdapterConfig(broker);
      stompConfig.setPort(STOMP_PORT);

      try (StompAdapter stompServer = HazelcastMQStomp.newStompAdapter(
          stompConfig)) {

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      // Create the backend worker.
      Backend backend = new Backend();
      Thread t = new Thread(backend);
      t.start();

      // Create a Stomp client.
      StompClient stompClient = new StompClient("localhost", STOMP_PORT);
      stompClient.connect();

      // Subscribe to the reply queue.
      StompClient.QueuingFrameListener msgListener
          = new StompClient.QueuingFrameListener();
      Frame frame = FrameBuilder.subscribe(REPLY_QUEUE, "sub-2").build();
      stompClient.subscribe(frame, msgListener);

      // Send a bunch of messages that need replies.
      for (int i = 0; i < 100; ++i) {

        log.info("Sending request frame number {}", i);

        // Build and send the request.
        frame = FrameBuilder.send(REQUEST_QUEUE, "Request " + i).header(
            "correlation-id", UUID.randomUUID().toString()).header("reply-to",
                REPLY_QUEUE).build();
        stompClient.send(frame);

        // Wait for the reply.
        frame = msgListener.poll(2, TimeUnit.SECONDS);
        if (frame == null) {
          log.warn("Did not get a reply frame!");
        } else {
          log.info("Got reply frame: {}", frame.getBodyAsString());
        }
      }

      // Shutdown the backend worker.
      backend.shutdown = true;
      t.join();

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

  private static class Backend implements Runnable {

    private transient boolean shutdown;

    @Override
    public void run() {
      int msgCount = 0;

      try {
        // Create a Stomp client.
        StompClient stompClient = new StompClient("localhost", STOMP_PORT);
        stompClient.connect();

        // Subscribe to a queue.
        StompClient.QueuingFrameListener msgListener
            = new StompClient.QueuingFrameListener();
        Frame frame = FrameBuilder.subscribe(REQUEST_QUEUE, "sub-1")
            .build();
        stompClient.subscribe(frame, msgListener);

        while (!shutdown && stompClient.isConnected()) {
          frame = msgListener.poll(2, TimeUnit.SECONDS);
          if (frame != null) {
            log.info("Got request frame {}. Sending reply.", frame
                .getBodyAsString());

            // Send a reply.
            String replyTo = frame.getHeaders().get("reply-to");
            String corrId = frame.getHeaders().get("correlation-id");

            frame = FrameBuilder.send(replyTo, "Reply " + msgCount).header(
                "correlation-id", corrId).build();
            stompClient.send(frame);
            msgCount++;
          }
        }

        stompClient.disconnect();
      }
      catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }
}
