package org.mpilone.hazelcastmq.stomper;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HazelcastMQStomper {

  private HazelcastMQStomperConfig config;
  private ServerSocket serverSocket;
  private boolean shutdown;
  private List<StomperClient> stomperClients;

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

  public void close() {
    shutdown = true;
    IoUtil.safeClose(serverSocket);
  }

  private void runServerLoop() {

    while (!shutdown) {
      try {
        Socket clientSocket = serverSocket.accept();
        onNewClient(clientSocket);
      }
      catch (Exception ex) {
        // Ignore for now
      }
    }

    List<StomperClient> clients = new ArrayList<StomperClient>(stomperClients);
    for (StomperClient client : clients) {
      client.close();
    }

  }

  public HazelcastMQStomperConfig getConfig() {
    return config;
  }

  private void onNewClient(Socket clientSocket) {
    try {
      stomperClients.add(new StomperClient(clientSocket, this));
    }
    catch (Throwable ex) {
      // TODO log this
    }
  }

  void onClientClosed(StomperClient client) {
    stomperClients.remove(client);
  }

}
