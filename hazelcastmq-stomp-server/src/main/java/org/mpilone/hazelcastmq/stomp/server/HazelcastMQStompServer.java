package org.mpilone.hazelcastmq.stomp.server;

import static java.lang.String.format;
import static org.mpilone.hazelcastmq.stomp.IoUtil.safeAwait;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.stomp.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A STOMP server backed by {@link HazelcastMQ}. The server is started
 * automatically at construction and will terminate when the {@link #shutdown()}
 * method is called.
  * 
 * @author mpilone
 */
public class HazelcastMQStompServer {

  /**
   * The configuration of the STOMP server.
   */
  private final HazelcastMQStompServerConfig config;

  /**
   * The server socket used to accept new connections.
   */
  private final ServerSocket serverSocket;

  /**
   * The flag which indicates if the server has been requested to shutdown.
   */
  private volatile boolean shutdown;

  /**
   * The shutdown latch that blocks shutdown until complete.
   */
  private final CountDownLatch shutdownLatch;

  /**
   * The list of actively connected clients.
   */
  private final List<ClientAgent> stomperClients;

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * Constructs the stomper STOMP server which will immediately begin listening
   * for connections on the configured port.
   * 
   * @param config
   *          the stomper configuration
   * @throws IOException
   *           if the server socket could not be properly initialized
   */
  public HazelcastMQStompServer(HazelcastMQStompServerConfig config)
      throws IOException {
    this.config = config;

    serverSocket = new ServerSocket(config.getPort());
    stomperClients = Collections.synchronizedList(new ArrayList<ClientAgent>());
    shutdownLatch = new CountDownLatch(1);

    config.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        runServerLoop();
      }
    });
  }

  /**
   * Shuts down the server socket. This method will block until the server is
   * shutdown completely.
   */
  public void shutdown() {
    shutdown = true;
    IoUtil.safeClose(serverSocket);
    safeAwait(shutdownLatch, 30, TimeUnit.SECONDS);
  }

  /**
   * Runs the loop accepting clients and spinning up client threads.
   */
  private void runServerLoop() {

    log.debug("Starting server loop.");

    while (!shutdown) {
      try {
        Socket clientSocket = serverSocket.accept();
        onNewClient(clientSocket);
      }
      catch (Exception ex) {
        log.debug("Server loop exception.", ex);
      }
    }

    log.debug("Stopping server loop. Shutting down clients.");

    List<ClientAgent> clients = new ArrayList<ClientAgent>(stomperClients);
    for (ClientAgent client : clients) {
      client.shutdown();
    }

    shutdownLatch.countDown();

    log.debug("Server loop complete.");
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
   * Called when a new client socket is accepted. A new {@link ClientAgent} is
   * constructed and should begin executing in a separate thread.
   * 
   * @param clientSocket
   *          the new socket that was just accepted
   */
  protected void onNewClient(Socket clientSocket) {
    try {
      log.debug("Creating new client.");
      stomperClients.add(new ClientAgent(clientSocket, this));
      log.info(format("A total of [%d] clients active.", stomperClients.size()));
    }
    catch (Throwable ex) {
      log.warn("Failed to start client session.", ex);
    }
  }

  /**
   * Called when a client closes. Every {@link ClientAgent} must call this
   * method to prevent resource leaks.
   * 
   * @param client
   *          the client that just closed
   */
  void onClientClosed(ClientAgent client) {
    stomperClients.remove(client);
  }
}
