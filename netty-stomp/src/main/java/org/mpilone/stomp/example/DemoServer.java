package org.mpilone.stomp.example;

import org.mpilone.stomp.client.BasicStompClient;
import org.mpilone.stomp.server.BasicStompServer;

/**
 *
 * @author mpilone
 */
public class DemoServer {

  public static void main(String[] args) throws InterruptedException {

    BasicStompServer server = new BasicStompServer();
    server.start(8090);

    BasicStompClient client = new BasicStompClient();
    client.connect("localhost", 8090);
    client.disconnect();

    server.stop();
  }

}
