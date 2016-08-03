package org.mpilone.hazelcastmq.example.stomp;

import org.mpilone.hazelcastmq.stomp.StompAdapterConfig;
import org.mpilone.hazelcastmq.stomp.StompAdapter;
import org.mpilone.hazelcastmq.stomp.HazelcastMQStomp;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.stomp.server.*;
import org.mpilone.yeti.*;
import org.mpilone.yeti.client.StompClient;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * This example uses a HazelcastMQ STOMP server to accept a Yeti STOMP client
 * connection simulating a frontend and a STOMP client connection from another
 * thread simulating a backend. The client then sends a bunch of request frames
 * and waits for replies. For each request/reply frame, the frontend client will
 * connect and disconnect to simulate a lot of client turnover. The server is
 * backed by HazelcastMQ which is backed by a local Hazelcast instance.
 *
 * @author mpilone
 */
public class StompToStompThreadedRequestReplyReconnect {

  /**
   * The log for this class.
   */
  private final static Logger log  = LoggerFactory.getLogger(
      StompToStompThreadedRequestReplyReconnect.class);

  private static final int STOMP_PORT = 8035;
  private static final String REQUEST_QUEUE = "/queue/do.some.work";
  private static final String REPLY_QUEUE = "/queue/do.some.work.replies";
  private static final String REPLY_BODY
      = "<p>I don't know what you're talking "
      + "about. I am a member of the Imperial Senate on a diplomatic mission to "
      + "Alderaan-- I suggest you try it again, Luke. This time, let go your "
      + "conscious self and act on instinct. What?! Don't act so surprised, "
      + "Your Highness. You weren't on any mercy mission this time. Several "
      + "transmissions were beamed to this ship by Rebel spies. I want to "
      + "know what happened to the plans they sent you. He is here.</p> <p>The "
      + "more you tighten your grip, Tarkin, the more star systems will slip "
      + "through your fingers. Remember, a Jedi can feel the Force flowing "
      + "through him. Oh God, my uncle. How am I ever gonna explain this? I "
      + "can't get involved! I've got work to do! It's not that I like the "
      + "Empire, I hate it, but there's nothing I can do about it right now. "
      + "It's such a long way from here. I can't get involved! I've got work "
      + "to do! It's not that I like the Empire, I hate it, but there's "
      + "nothing I can do about it right now. It's such a long way from "
      + "here.</p> <p>The Force is strong with this one. I have you now. "
      + "The more you tighten your grip, Tarkin, the more star systems will "
      + "slip through your fingers. Dantooine. They're on Dantooine.</p> "
      + "<p>You mean it controls your actions? All right. Well, take care of "
      + "yourself, Han. I guess that's what you're best at, ain't it? Hokey "
      + "religions and ancient weapons are no match for a good blaster at "
      + "your side, kid. No! Alderaan is peaceful. We have no weapons. You "
      + "can't possibly&hellip; Ye-ha! Obi-Wan is here. The Force is with "
      + "him.</p> <p>Look, I can take you as far as Anchorhead. You can get "
      + "a transport there to Mos Eisley or wherever you're going. Leave that "
      + "to me. Send a distress signal, and inform the Senate that all on "
      + "board were killed. Oh God, my uncle. How am I ever gonna explain "
      + "this? Hokey religions and ancient weapons are no match for a good "
      + "blaster at your side, kid. As you wish.</p> ";

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    new StompToStompThreadedRequestReplyReconnect();
  }

  /**
   * Constructs and executes the example.
   *
   * @throws Exception if there is an unexpected error
   */
  public StompToStompThreadedRequestReplyReconnect() throws Exception {

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
      StompAdapterConfig stompConfig = new StompAdapterConfig(
          mqInstance);
      stompConfig.setPort(STOMP_PORT);
      StompAdapter stompServer = HazelcastMQStomp.newStompAdapter(stompConfig);

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      // Create the backend worker.
      Backend backend = new Backend();
      Thread t = new Thread(backend);
      t.start();

      // Send a bunch of messages that need replies.
      for (int i = 0; i < 50; ++i) {

        // Create a Stomp client.
        StompClient stompClient = new StompClient("localhost",
            STOMP_PORT);
        stompClient.connect();

        // Subscribe to the reply queue.
        StompClient.QueuingFrameListener msgListener
            = new StompClient.QueuingFrameListener();
        Frame frame = FrameBuilder.subscribe(REPLY_QUEUE, "sub-2").build();
        stompClient.subscribe(frame, msgListener);

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

        // Shutdown the client.
        stompClient.disconnect();
      }

      // Shutdown the backend worker.
      backend.shutdown = true;
      t.join();


      // Shutdown the server.
      log.info("Shutting down STOMP server.");
      stompServer.shutdown();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }

  }

  /**
   * Simulates a backend worker that operates in a different thread or process.
   */
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

            String body = "Reply " + msgCount + ": " + REPLY_BODY;

            frame = FrameBuilder.send(replyTo, body).header("correlation-id", corrId).build();
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
