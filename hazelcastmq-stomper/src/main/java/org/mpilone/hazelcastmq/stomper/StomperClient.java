package org.mpilone.hazelcastmq.stomper;

import java.io.*;
import java.net.Socket;

import javax.jms.*;

class StomperClient {
  private Socket clientSocket;
  private HazelcastMQStomper stomper;
  private FrameInputStream instream;
  private FrameOutputStream outstream;
  private boolean connected = false;
  private Connection connection;
  private HazelcastMQStomperConfig config;
  private boolean shutdown;

  public StomperClient(Socket clientSocket, HazelcastMQStomper stomper)
      throws IOException, JMSException {
    this.clientSocket = clientSocket;
    this.stomper = stomper;

    instream = new FrameInputStream(clientSocket.getInputStream());
    outstream = new FrameOutputStream(clientSocket.getOutputStream());

    config = this.stomper.getConfig();
    connection = config.getConnectionFactory().createConnection();
    shutdown = false;

    config.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        runClientLoop();
      }
    });
  }

  public void runClientLoop() {

    Frame frame = null;
    while (!shutdown) {
      try {
        frame = instream.read();
        shutdown = !processFrame(frame);
      }
      catch (Throwable ex) {
        if (!shutdown) {
          // Try sending the error frame.
          sendError(frame, ex);
        }
      }
    }

    connected = false;

    // TODO close all consumers.
    IoUtil.safeClose(connection);

    IoUtil.safeClose(instream);
    IoUtil.safeClose(outstream);
    IoUtil.safeClose(clientSocket);

    stomper.onClientClosed(this);
  }

  public void close() {
    shutdown = true;
    IoUtil.safeClose(clientSocket);
  }

  private void sendError(Frame frame, Throwable ex) {
    try {
      Frame response = new Frame(Command.ERROR);
      response.getHeaders().put("message", ex.getMessage());
      response.setContentTypeText();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      FrameOutputStream frameBuf = new FrameOutputStream(buf);
      frameBuf.write(frame, false);
      frameBuf.close();

      byte[] origFrameData = buf.toByteArray();

      buf = new ByteArrayOutputStream();
      PrintWriter writerBuf = new PrintWriter(new OutputStreamWriter(buf,
          "UTF-8"));
      writerBuf.write("The original message:\n");
      writerBuf.write("----------------\n");
      writerBuf.flush();
      buf.write(origFrameData);
      writerBuf.write("\n----------------\n");
      writerBuf.close();

      response.setBody(buf.toByteArray());
      guardedWrite(response);
    }
    catch (Throwable ex2) {
      // Ignore
    }
  }

  private boolean processFrame(Frame frame) throws IOException, JMSException {

    switch (frame.getCommand()) {
    case CONNECT:
      onConnect(frame);
      break;

    case DISCONNECT:
      onDisconnect(frame);
      return false;

    case SEND:
      onSend(frame);
      break;

    default:
      throw new RuntimeException("Command not currently supported.");
    }
    // TODO add more supported commands

    return true;
  }

  private void onSend(Frame frame) throws IOException, JMSException {

    String destName = frame.getHeaders().get("destination");
    if (destName == null) {
      throw new StompException("Destination header is required.");
    }

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = config.getFrameConverter().fromFrameDestination(
        destName, session);
    MessageProducer producer = session.createProducer(destination);
    producer.send(config.getFrameConverter().fromFrame(frame, session));

    IoUtil.safeClose(producer);
    IoUtil.safeClose(session);

    if (frame.getHeaders().containsKey("receipt")) {
      Frame response = new Frame(Command.RECEIPT);
      response.getHeaders()
          .put("receipt-id", frame.getHeaders().get("receipt"));
      guardedWrite(response);
    }

  }

  private void onDisconnect(Frame frame) throws IOException {
    connected = false;

    Frame response = new Frame(Command.RECEIPT);
    response.getHeaders().put("receipt-id", frame.getHeaders().get("receipt"));
    guardedWrite(response);
  }

  private void onConnect(Frame frame) throws IOException {
    // TODO add version negotiation and heart-beat support.

    connected = true;

    Frame response = new Frame(Command.CONNECTED);
    response.getHeaders().put("version", "1.2");
    response.setContentTypeText();
    guardedWrite(frame);

  }

  private void guardedWrite(Frame frame) throws IOException {
    synchronized (outstream) {
      outstream.write(frame);
    }
  }
}
