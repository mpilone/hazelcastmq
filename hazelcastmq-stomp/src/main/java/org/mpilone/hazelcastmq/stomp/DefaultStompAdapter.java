package org.mpilone.hazelcastmq.stomp;


import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.yeti.Stomplet;
import org.mpilone.yeti.server.StompServer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A STOMP server backed by {@link HazelcastMQ}. The server is started
 * automatically at construction and will terminate when the {@link #close()}
 * method is called. This implementation is a thin wrapper on
 * {@link StompServer} that simply configures the server to use the
 * {@link StompAdapterStomplet}. It is possible (and even recommended) that the
 * stomplet be used directly to allow for more specific Yeti/Netty server
 * configuration.
  * 
 * @author mpilone
 */
public class DefaultStompAdapter implements StompAdapter {

  /**
   * The log for this class.
   */
  private final ILogger log = Logger.getLogger(DefaultStompAdapter.class);

  /**
   * The configuration of the STOMP server.
   */
  private final StompAdapterConfig config;

  /**
   * The STOMP server that will relay messages into HazelcastMQ.
   */
  private final StompServer stompServer;

  /**
   * Constructs the STOMP server which will immediately begin listening
   * for connections on the configured port.
   * 
   * @param config
   *          the stomper configuration
   */
  DefaultStompAdapter(final StompAdapterConfig config) {
    this.config = config;
    this.stompServer = new StompServer(
        this.config.getMaxFrameSize(),
        this.config.getPort(),
        new HazelcastMQStompletFactory());

    try {
      // Bind and start to accept incoming connections.
      stompServer.start();
    }
    catch (InterruptedException ex) {
      log.warning("Interrupted while starting up. "
          + "Startup may not be complete.", ex);
    }
  }

  @Override
  public void close() {
    try {
      // Wait until the server socket is closed.
     stompServer.stop();
    }
    catch (InterruptedException ex) {
      log.warning("Interrupted while shutting down. "
          + "Shutdown may not be complete.", ex);
    }
  }

  /**
   * Returns the STOMP configuration. Alterations to the configuration after the
   * server has started may not have an affect.
    * 
   * @return the configuration given during construction
   */
  public StompAdapterConfig getConfig() {
    return config;
  }

  /**
   * A {@link StompServer.StompletFactory} that returns new instances of
   * {@link StompAdapterStomplet}s.
   */
  private class HazelcastMQStompletFactory implements
      StompServer.StompletFactory {

    @Override
    public Stomplet createStomplet() throws Exception {
      return new StompAdapterStomplet(config);
    }

  }
}
