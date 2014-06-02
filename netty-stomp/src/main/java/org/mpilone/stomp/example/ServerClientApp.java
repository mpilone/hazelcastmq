package org.mpilone.stomp.example;

import org.mpilone.stomp.*;
import org.mpilone.stomp.client.BasicStompClient;
import org.mpilone.stomp.server.*;

/**
 *
 * @author mpilone
 */
public class ServerClientApp {

  public static void main(String[] args) throws InterruptedException {

    StompServer server = StompServerBuilder.port(8090).frameDebug(true).
        stompletClass(InMemoryBrokerStomplet.class).build();
    server.start();

    BasicStompClient client1 = new BasicStompClient();
    client1.connect("localhost", 8090);
    client1.write(FrameBuilder.subscribe("foo.bar", "client1-1").build());
    client1.setFrameListener(          new BasicStompClient.FrameListener() {
            public void frameReceived(Frame frame) {
              System.out.println("Received message: " + frame.getBodyAsString());
            }
          });

    BasicStompClient client2 = new BasicStompClient();
    client2.connect("localhost", 8090);
    client2.write(FrameBuilder.send("foo.bar", "Hello").build());
    client2.write(FrameBuilder.send("foo.poo", "Goodbye").build());
    client2.write(FrameBuilder.send("foo.bar", "World!").build());

    try {
      // Wait for the messages to arrive.
      Thread.sleep(2000);
    }
    catch (InterruptedException ex) {
      // ignore
    }

    client2.disconnect();
    client1.disconnect();

    server.stop();
  }

}
