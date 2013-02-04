package org.mpilone.hazelcastmq.stomper;

import static java.lang.String.format;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A STOMP server which uses a JMS {@link ConnectionFactory} to create message
 * producers and consumers. The server is started automatically at construction
 * and will terminate when the {@link #shutdown()} method is called.
 * 
 * @author mpilone
 * 
 */
public class HazelcastMQStomper {

  /**
   * The configuration of the STOMP server.
   */
  private HazelcastMQStomperConfig config;

  /**
   * The server socket used to accept new connections.
   */
  private ServerSocket serverSocket;

  /**
   * The flag which indicates if the server has been requested to shutdown.
   */
  private volatile boolean shutdown;

  /**
   * The list of actively connected clients.
   */
  private List<StomperClient> stomperClients;

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
  public HazelcastMQStomper(HazelcastMQStomperConfig config) throws IOException {
    this.config = config;

    serverSocket = new ServerSocket(config.getPort());
    stomperClients = Collections
        .synchronizedList(new ArrayList<StomperClient>());

    config.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        runServerLoop();
      }
    });
  }

  /**
   * Shuts down the server socket. This method will return immediately. To block
   * or wait for a complete shutdown, shutdown and wait on the
   * {@link HazelcastMQStomperConfig#getExecutor()} instance.
   */
  public void shutdown() {
    shutdown = true;
    IoUtil.safeClose(serverSocket);
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

    log.debug("Stopping server loop. Closing clients.");

    List<StomperClient> clients = new ArrayList<StomperClient>(stomperClients);
    for (StomperClient client : clients) {
      client.close();
    }

    log.debug("Server loop complete.");

  }

  /**
   * Returns the stomper configuration. Alterations to the configuration after
   * the server has started may not have an affect.
   * 
   * @return the configuration given during construction
   */
  public HazelcastMQStomperConfig getConfig() {
    return config;
  }

  /**
   * Called when a new client socket is accepted. A new {@link StomperClient} is
   * constructed and should begin executing in a separate thread.
   * 
   * @param clientSocket
   *          the new socket that was just accepted
   */
  protected void onNewClient(Socket clientSocket) {
    try {
      log.debug("Creating new client.");
      stomperClients.add(new StomperClient(clientSocket, this));
      log.info(format("A total of [%d] clients active.", stomperClients.size()));
    }
    catch (Throwable ex) {
      log.warn("Failed to start client session.", ex);
    }
  }

  /**
   * Called when a client closes. Every {@link StomperClient} must call this
   * method to prevent resource leaks.
   * 
   * @param client
   *          the client that just closed
   */
  void onClientClosed(StomperClient client) {
    stomperClients.remove(client);
  }
}
