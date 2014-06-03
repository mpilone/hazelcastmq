package org.mpilone.yeti.server;


import org.mpilone.yeti.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 *
 * @author mpilone
 */
public class StompServer {

  private Channel channel;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;

  private StompletFactory stompletFactory;
  private int port;
  private boolean frameDebugEnabled;

  public void start() throws InterruptedException {
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(createChildHandler())
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    ChannelFuture f = b.bind(port).sync();
    channel = f.channel();
  }

  public void stop() throws InterruptedException {
    try {
      // Wait until the server socket is closed.
      if (channel.isActive()) {
        channel.close().sync();
      }
    }
    finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();

      workerGroup = null;
      bossGroup = null;
      channel = null;
    }
  }

  protected ChannelHandler createChildHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new StompFrameDecoder());
        ch.pipeline().addLast(new StompFrameEncoder());

        if (frameDebugEnabled) {
          ch.pipeline().addLast(new FrameDebugHandler());
        }

        // Create a new stomplet instance for each client connection.
        
        ch.pipeline().addLast(new StompletFrameHandler(stompletFactory.
            createStomplet()));
      }
    };
  }

  public void setFrameDebugEnabled(boolean frameDebugEnabled) {
    this.frameDebugEnabled = frameDebugEnabled;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setStompletFactory(StompletFactory stompletFactory) {
    this.stompletFactory = stompletFactory;
  }

  public interface StompletFactory {
    Stomplet createStomplet() throws Exception;
  }
}
