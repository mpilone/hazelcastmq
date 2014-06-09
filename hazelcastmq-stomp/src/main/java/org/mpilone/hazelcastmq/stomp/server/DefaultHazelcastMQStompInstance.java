package org.mpilone.hazelcastmq.stomp.server;


import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.yeti.Stomplet;
import org.mpilone.yeti.server.StompServer;
import org.slf4j.*;

/**
 * A STOMP server backed by {@link HazelcastMQ}. The server is started
 * automatically at construction and will terminate when the {@link #shutdown()}
 * method is called.
  * 
 * @author mpilone
 */
public class DefaultHazelcastMQStompInstance implements HazelcastMQStompInstance {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(
      DefaultHazelcastMQStompInstance.class);

  /**
   * The configuration of the STOMP server.
   */
  private final HazelcastMQStompConfig config;

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
   */
  DefaultHazelcastMQStompInstance(final HazelcastMQStompConfig config) {
    this.config = config;
    this.stompServer = new StompServer(this.config.isFrameDebugEnabled(),
        this.config.getPort(),
        new HazelcastMQStompletFactory());

    try {
      // Bind and start to accept incoming connections.
      stompServer.start();
    }
    catch (InterruptedException ex) {
      log.warn("Interrupted while starting up. "
          + "Startup may not be complete.", ex);
    }
  }

  @Override
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
  public HazelcastMQStompConfig getConfig() {
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
