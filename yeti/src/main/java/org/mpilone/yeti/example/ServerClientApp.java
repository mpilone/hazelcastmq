package org.mpilone.yeti.example;

import java.util.concurrent.TimeUnit;

import org.mpilone.yeti.Frame;
import org.mpilone.yeti.FrameBuilder;
import org.mpilone.yeti.client.StompClient;
import org.mpilone.yeti.client.StompClientBuilder;
import org.mpilone.yeti.server.InMemoryBrokerStomplet;
import org.mpilone.yeti.server.StompServer;
import org.mpilone.yeti.server.StompServerBuilder;

/**
 *
 * @author mpilone
 */
public class ServerClientApp {

  public static void main(String[] args) throws InterruptedException {

    int port = 8090;

    StompServer server = StompServerBuilder.port(port).frameDebug(true).
        stompletClass(InMemoryBrokerStomplet.class).build();
    server.start();

    StompClient.QueuingFrameListener msgListener =
        new StompClient.QueuingFrameListener();

    StompClient client1 =
        StompClientBuilder.port(port).host("localhost").frameDebug(true).build();
    client1.connect();
    client1.subscribe(FrameBuilder.subscribe("foo.bar", "client1-1").build(),
        msgListener);

    StompClient client2 = StompClientBuilder.port(port).host("localhost").
        frameDebug(true).build();
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

    server.stop();
  }

}
