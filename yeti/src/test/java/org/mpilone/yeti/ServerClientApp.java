package org.mpilone.yeti;

import java.util.concurrent.TimeUnit;

import org.mpilone.yeti.client.StompClient;
import org.mpilone.yeti.server.InMemoryBrokerStomplet;
import org.mpilone.yeti.server.StompServer;

/**
 * An example Yeti STOMP server with multiple clients. The
 * {@link InMemoryBrokerStomplet} is used to support simple message passing
 * between connected clients.
 *
 * @author mpilone
 */
public class ServerClientApp {

  public static void main(String[] args) throws InterruptedException {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    int port = 8090;

    StompServer server = new StompServer(StompFrameDecoder.DEFAULT_MAX_FRAME_SIZE, port,
        new StompServer.ClassStompletFactory(InMemoryBrokerStomplet.class));
    server.start();

    try {
      StompClient.QueuingFrameListener msgListener =
          new StompClient.QueuingFrameListener();

      StompClient client1 = new StompClient(true, "localhost", port);
      client1.connect();
      client1.subscribe(FrameBuilder.subscribe("foo.bar", "client1-1").build(),
          msgListener);

      StompClient client2 = new StompClient(true, "localhost", port);
      client2.connect();
      client2.send(FrameBuilder.send("foo.bar", "Hello").build());
      client2.send(FrameBuilder.send("foo.poo", "Goodbye").build());
      client2.send(FrameBuilder.send("foo.bar", "World!").build());
      client2.disconnect();

      try {
        // Wait for the messages to arrive. We should get two of them.
        Frame msg = msgListener.poll(2, TimeUnit.SECONDS);
        System.out.println("Got message 1: " + msg.getBodyAsString());

        msg = msgListener.poll(2, TimeUnit.SECONDS);
        System.out.println("Got message 2: " + msg.getBodyAsString());
      }
      catch (InterruptedException ex) {
        // ignore
      }

      client1.unsubscribe(FrameBuilder.unsubscribe("client1-1").build());
      client1.disconnect();
    }
    finally {
      server.stop();
    }
  }

}
