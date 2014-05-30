package org.mpilone.hazelcastmq.stomp.server;


import java.io.IOException;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.stomp.server.*;
import org.mpilone.stomp.StompFrameDecoder;
import org.mpilone.stomp.StompFrameEncoder;
import org.slf4j.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * A STOMP server backed by {@link HazelcastMQ}. The server is started
 * automatically at construction and will terminate when the {@link #shutdown()}
 * method is called.
  * 
 * @author mpilone
 */
public class HazelcastMQStompServer {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(
      HazelcastMQStompServer.class);

  /**
   * The configuration of the STOMP server.
   */
  private final HazelcastMQStompServerConfig config;

  /**
   * The server channel used to accept new connections.
   */
  private Channel channel;

  private  NioEventLoopGroup bossGroup;
  private  NioEventLoopGroup workerGroup;

  /**
   * Constructs the stomper STOMP server which will immediately begin listening
   * for connections on the configured port.
   * 
   * @param config
   *          the stomper configuration
   * @throws IOException
   *           if the server socket could not be properly initialized
   */
  public HazelcastMQStompServer(final HazelcastMQStompServerConfig config)
      throws IOException {
    this.config = config;

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
            ch.pipeline().addLast(new SendFrameHandler(config));
            ch.pipeline().addLast(new SubscribeFrameHandler(config));
            ch.pipeline().addLast(new ReceiptWritingHandler());
            ch.pipeline().addLast(new DisconnectFrameHandler());
            ch.pipeline().addLast(new ErrorWritingHandler());
          }
        })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    try {
      // Bind and start to accept incoming connections.
      ChannelFuture f = b.bind(config.getPort()).sync();
      channel = f.channel();
    }
    catch (InterruptedException ex) {
      log.warn("Interrupted while starting up. "
          + "Startup may not be complete.", ex);
    }
  }

  /**
   * Shuts down the server socket. This method will block until the server is
   * shutdown completely.
   */
  public void shutdown() {
    try {
      // Wait until the server socket is closed.
      if (channel.isActive()) {
        channel.close().sync();
      }
    }
    catch (InterruptedException ex) {
      log.warn("Interrupted while shutting down. "
          + "Shutdown may not be complete.", ex);
    }
    finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();

      workerGroup = null;
      bossGroup = null;
      channel = null;
    }
  }

  /**
   * Returns the stomper configuration. Alterations to the configuration after
   * the server has started may not have an affect.
   * 
   * @return the configuration given during construction
   */
  public HazelcastMQStompServerConfig getConfig() {
    return config;
  }
}
