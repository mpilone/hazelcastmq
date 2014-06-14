package org.mpilone.yeti.server;



import org.mpilone.yeti.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * <p>
 * A simple STOMP server that uses Netty to bind to a port and listen for
 * incoming frames. The server builds a pipeline containing a
 * {@link StompletFrameHandler} to delegate all frames to a {@link Stomplet}
 * implementation.
 * </p>
 * <p>
 * This class is a simple convenience implementation of a server, but it isn't
 * much more than a thin layer over a standard Netty server configuration.
 * Therefore it is recommended to implement a custom Netty configuration in a
 * production environment and simply use the Frame codecs and handlers to
 * construct a pipeline. This allows for more complete control over the
 * low-level network IO and configuration as well as support for any custom
 * channel handlers.
 * </p>
 *
 * @author mpilone
 */
public class StompServer {

  private Channel channel;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;

  private final StompletFactory stompletFactory;
  private final int port;
  private final boolean frameDebugEnabled;

  /**
   * Constructs the server which will bind on the given port and use the
   * stomplet factory to create new stomplet instances for client connections.
   *
   * @param port the port to bind to
   * @param stompletFactory the factory to create a new stomplet per client
   */
  public StompServer(int port, StompletFactory stompletFactory) {
    this(false, port, stompletFactory);
  }

  /**
   * Constructs the server which will bind on the given port and use the
   * stomplet factory to create new stomplet instances for client connections.
   *
   * @param frameDebugEnabled true to enable frame debugging
   * @param port the port to bind to
   * @param stompletFactory the factory to create a new stomplet per client
   */
  public StompServer(boolean frameDebugEnabled, int port,
      StompletFactory stompletFactory) {
    this.frameDebugEnabled = frameDebugEnabled;
    this.port = port;
    this.stompletFactory = stompletFactory;
  }

  /**
   * Starts the server, blocking until fully initialized and bound.
   *
   * @throws InterruptedException if the startup is interrupted
   */
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

  /**
   * Stops the server, blocking until fully stopped.
   *
   * @throws InterruptedException if the shutdown is interrupted
   */
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

  /**
   * Creates the child channel handler. By default a {@link ChannelInitializer}
   * is created which will construct a pipeline of
   * {@link StompFrameDecoder}, {@link StompFrameEncoder}, {@link FrameDebugHandler},
   * and {@link StompletFrameHandler}.
   *
   * @return the channel handler for child channel (i.e. client connection)
   * construction
   */
  protected ChannelHandler createChildHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(StompFrameDecoder.class.getName(),
            new StompFrameDecoder());
        ch.pipeline().addLast(StompFrameEncoder.class.getName(),
            new StompFrameEncoder());

        if (frameDebugEnabled) {
          ch.pipeline().addLast(FrameDebugHandler.class.getName(),
              new FrameDebugHandler(true, true));
        }

        // Create a new stomplet instance for each client connection.
        ch.pipeline().addLast(StompletFrameHandler.class.getName(),
            new StompletFrameHandler(stompletFactory.createStomplet()));
      }
    };
  }

  /**
   * A simple factory to create a stomplet instance for each new client
   * connection.
   */
  public interface StompletFactory {

    /**
     * Creates a new stomplet instance.
     *
     * @return the new instance
     * @throws Exception if an error occurs
     */
    Stomplet createStomplet() throws Exception;
  }

  /**
   * A stomplet factory that simply calls {@link Class#newInstance() } to create
   * a stomplet.
   */
  public static class ClassStompletFactory implements StompletFactory {

    private final Class<? extends Stomplet> stompletClass;

    /**
     * Constructs the factory.
     *
     * @param stompletClass the stomplet class to instantiate
     */
    public ClassStompletFactory(
        Class<? extends Stomplet> stompletClass) {
      this.stompletClass = stompletClass;
    }

    @Override
    public Stomplet createStomplet() throws Exception {
      return stompletClass.newInstance();
    }
  }
}
