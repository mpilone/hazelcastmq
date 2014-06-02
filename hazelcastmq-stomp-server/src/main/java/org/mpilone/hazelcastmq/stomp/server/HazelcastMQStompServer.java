package org.mpilone.hazelcastmq.stomp.server;


import java.io.IOException;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.stomp.Stomplet;
import org.mpilone.stomp.server.*;
import org.slf4j.*;

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
   * The STOMP server that will relay messages into HazelcastMQ.
   */
  private final StompServer stompServer;

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
    this.stompServer = StompServerBuilder.port(this.config.getPort()).
        frameDebug(false).stompletFactory(new HazelcastMQStompletFactory()).
        build();

    try {
      // Bind and start to accept incoming connections.
      stompServer.start();
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
     stompServer.stop();
    }
    catch (InterruptedException ex) {
      log.warn("Interrupted while shutting down. "
          + "Shutdown may not be complete.", ex);
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

  /**
   * A {@link StompServer.StompletFactory} that returns new instances of
   * {@link HazelcastMQStomplet}s.
   */
  private class HazelcastMQStompletFactory implements
      StompServer.StompletFactory {

    @Override
    public Stomplet createStomplet() throws Exception {
      return new HazelcastMQStomplet(config);
    }

  }
}
