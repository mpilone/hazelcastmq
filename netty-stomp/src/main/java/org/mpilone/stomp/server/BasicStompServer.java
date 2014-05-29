
package org.mpilone.stomp.server;

import org.mpilone.stomp.shared.StompFrameDecoder;
import org.mpilone.stomp.shared.StompFrameEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 *
 * @author mpilone
 */
public class BasicStompServer {
  private Channel channel;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;

  public void start(int port) throws InterruptedException {
    bossGroup = new NioEventLoopGroup(); 
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap b = new ServerBootstrap(); 
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class) 
        .childHandler(new ChannelInitializer<SocketChannel>() { 
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new StompFrameDecoder());
            ch.pipeline().addLast(new StompFrameEncoder());
            ch.pipeline().addLast(new ConnectFrameHandler());
            ch.pipeline().addLast(new ReceiptWriteHandler());
            ch.pipeline().addLast(new ErrorWriteHandler());
            ch.pipeline().addLast(new DisconnectFrameHandler());
          }
        })
        .option(ChannelOption.SO_BACKLOG, 128) 
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    ChannelFuture f = b.bind(port).sync(); 
    channel = f.channel();
  }

  public void stop() throws InterruptedException {
    try {
      // Wait until the server socket is closed.
      // In this example, this does not happen, but you can do that to gracefully
      // shut down your server.
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

}
