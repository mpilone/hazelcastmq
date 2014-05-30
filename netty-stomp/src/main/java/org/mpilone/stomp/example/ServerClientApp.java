package org.mpilone.stomp.example;

import org.mpilone.stomp.*;
import org.mpilone.stomp.client.BasicStompClient;
import org.mpilone.stomp.server.*;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 *
 * @author mpilone
 */
public class ServerClientApp {

  public static void main(String[] args) throws InterruptedException {

    DemoStompServer server = new DemoStompServer();
    server.start(8090);

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

  private static class DemoStompServer extends BasicStompServer {

    @Override
    protected ChannelHandler createChildHandler() {
      final InMemoryBroker broker = new InMemoryBroker();

      return new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
          ch.pipeline().addLast(new StompFrameDecoder());
          ch.pipeline().addLast(new StompFrameEncoder());

          ch.pipeline().addLast(new FrameDebugHandler());
          ch.pipeline().addLast(new ConnectFrameHandler());
          ch.pipeline().addLast(new SendFrameHandler(broker));
          ch.pipeline().addLast(new SubscribeFrameHandler(broker));
          ch.pipeline().addLast(new ReceiptWritingHandler());
          ch.pipeline().addLast(new DisconnectFrameHandler());
          ch.pipeline().addLast(new ErrorWritingHandler());
        }
      };
    }
  }
}
