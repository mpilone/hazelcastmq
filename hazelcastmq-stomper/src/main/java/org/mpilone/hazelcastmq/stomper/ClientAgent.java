package org.mpilone.hazelcastmq.stomper;

import static java.lang.String.format;
import static org.mpilone.hazelcastmq.stomp.IoUtil.safeAwait;
import static org.mpilone.hazelcastmq.stomp.IoUtil.safeClose;
import static org.mpilone.hazelcastmq.stomper.JmsUtil.safeClose;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import org.mpilone.hazelcastmq.stomp.*;
import org.mpilone.hazelcastmq.stomper.ClientSubscription.MessageCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Stomper client connection which handles processing all STOMP commands for
 * the client using frame input and output streams.
 * 
 * @author mpilone
 */
class ClientAgent {
  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The low level network socket to which the client is connected.
   */
  private Socket clientSocket;

  /**
   * The parent stomper instance.
   */
  private HazelcastMQStomper stomper;

  /**
   * The frame input stream for reading frames from the client.
   */
  private FrameInputStream instream;

  /**
   * The frame output stream for writing frames to the client.
   */
  private FrameOutputStream outstream;

  /**
   * The flag which indicates if the CONNECT negotiation is complete.
   */
  private boolean connected = false;

  /**
   * The JMS connection for all JMS sending/receiving.
   */
  private Connection connection;

  /**
   * The configuration cached from the {@link #stomper} instance.
   */
  private HazelcastMQStomperConfig config;

  /**
   * The flag which indictes if a shutdown has been requested.
   */
  private volatile boolean shutdown;

  /**
   * The shutdown latch that blocks shutdown until complete.
   */
  private CountDownLatch shutdownLatch;

  /**
   * The map of subscription ID to the subscription instance for all active
   * client subscriptions.
   */
  private Map<String, ClientSubscription> subscriptions;

  /**
   * The callback that will handle all messages received from subscriptions and
   * need to get dispatched to the client.
   */
  private MessageCallback messageCallback;

  /**
   * The default implementation of the message callback. This implementation
   * converts the JMS message into a STOMP frame and writes a
   * {@link Command#MESSAGE} frame to the client.
   * 
   * @author mpilone
   */
  private class DefaultMessageCallback implements MessageCallback {
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.stomper.StomperClientSubscription.MessageCallback
     * #onMessage(org.mpilone.hazelcastmq.stomper.StomperClientSubscription,
     * javax.jms.Message)
     */
    @Override
    public void onMessage(ClientSubscription subscription, Message msg) {
      try {
        Frame frame = config.getFrameConverter().toFrame(msg);
        frame.getHeaders()
            .put("subscription", subscription.getSubscriptionId());

        outstream.write(frame);
      }
      catch (Throwable ex) {
        // Ignore
        log.debug("Exception while building message frame for client.", ex);
      }
    }
  }

  /**
   * Constructs the client which will read and write from the given socket. The
   * client will immediately start servicing requests in a separate thread.
   * 
   * @param clientSocket
   *          the client socket to read from and write to
   * @param stomper
   *          the parent stomper instance
   * @throws IOException
   * @throws JMSException
   */
  public ClientAgent(Socket clientSocket, HazelcastMQStomper stomper)
      throws IOException, JMSException {
    this.clientSocket = clientSocket;
    this.stomper = stomper;

    instream = new FrameInputStream(clientSocket.getInputStream());
    outstream = new FrameOutputStream(clientSocket.getOutputStream());

    config = this.stomper.getConfig();
    connection = config.getConnectionFactory().createConnection();
    connection.start();

    shutdown = false;
    subscriptions = new HashMap<String, ClientSubscription>();
    messageCallback = new DefaultMessageCallback();
    shutdownLatch = new CountDownLatch(1);

    config.getExecutor().execute(new Runnable() {
      @Override
      public void run() {
        runClientLoop();
      }
    });
  }

  /**
   * Runs the main client loop, reading and writing frames. The loop will run
   * until a shutdown is requested or an exception is raised.
   */
  public void runClientLoop() {

    log.debug("Starting client loop.");

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
        shutdown = true;
      }
    }

    log.debug("Stopping client loop.");

    connected = false;
    shutdown = true;

    // Close all consumers/subscriptions.
    for (ClientSubscription subscription : subscriptions.values()) {
      safeClose(subscription.getConsumer());
      safeClose(subscription.getSession());
    }
    subscriptions.clear();

    // Close the JMS connection.
    safeClose(connection);

    // Close the socket connection.
    safeClose(instream);
    safeClose(outstream);
    safeClose(clientSocket);

    // Notify the stomper server that we're done.
    stomper.onClientClosed(this);

    shutdownLatch.countDown();

    log.debug("Client loop complete.");
  }

  /**
   * Closes this client. This method will block until the client is completely
   * shutdown.
   */
  public void shutdown() {
    shutdown = true;
    safeClose(clientSocket);
    safeAwait(shutdownLatch, 30, TimeUnit.SECONDS);
  }

  /**
   * Sends a {@link Command#ERROR} frame to the client using given frame as the
   * cause/body of the error frame.
   * 
   * @param requestFrame
   *          the frame that triggered the error or was being processed when the
   *          error occurred
   * @param cause
   *          the exception to relay
   */
  private void sendError(Frame requestFrame, Throwable cause) {
    try {

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      FrameOutputStream frameBuf = new FrameOutputStream(buf);
      frameBuf.write(requestFrame, false);
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

      FrameBuilder fb = FrameBuilder.command(Command.ERROR)
          .header("message", cause.getMessage()).headerContentTypeText()
          .body(buf.toString());
      outstream.write(fb.build());
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  /**
   * Checks that the connected state of the client matches the given expected
   * state.
   * 
   * @param expectedConnected
   *          the expected connected state
   * @throws StompException
   *           if the connected state doesn't match the expected state
   */
  private void checkConnected(boolean expectedConnected) throws StompException {

    if (expectedConnected && !connected) {
      throw new StompException("Command is only permitted after connection.");
    }
    else if (!expectedConnected && connected) {
      throw new StompException("Command is only permitted before connection.");
    }
  }

  /**
   * Processes the given frame based on the frame's command. If the frame
   * requires a response, it will be written to the client.
   * 
   * @param frame
   *          the frame to process
   * @return true if processing should continue, false if the client should be
   *         terminated
   * @throws IOException
   * @throws JMSException
   */
  private boolean processFrame(Frame frame) throws IOException, JMSException {

    // Process the specific command.
    switch (frame.getCommand()) {
    case STOMP:
    case CONNECT:
      checkConnected(false);
      onConnect(frame);
      break;

    case DISCONNECT:
      checkConnected(true);
      onDisconnect(frame);
      return false;

    case SEND:
      checkConnected(true);
      onSend(frame);
      break;

    case SUBSCRIBE:
      checkConnected(true);
      onSubscribe(frame);
      break;

    case UNSUBSCRIBE:
      checkConnected(true);
      onUnsubscribe(frame);
      break;

    // TODO add more supported commands

    default:
      throw new StompException(format("Command [%s] not currently supported.",
          frame.getCommand()));
    }

    return true;
  }

  /**
   * Called when a {@link Command#SEND} frame is received from the client. The
   * frame will be converted to a JMS {@link Message} and dispatched to the
   * appropriate destination.
   * 
   * @param frame
   *          the frame to process
   * @throws IOException
   * @throws JMSException
   */
  private void onSend(Frame frame) throws IOException, JMSException {

    // Get the destination header.
    String destName = getRequiredHeader("destination", frame);

    // Create the JMS components.
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = config.getFrameConverter().fromFrameDestination(
        destName, session);
    MessageProducer producer = session.createProducer(destination);

    // Convert and send the message.
    producer.send(config.getFrameConverter().fromFrame(frame, session));

    // Cleanup.
    safeClose(producer);
    safeClose(session);

    // Send a receipt if the client asked for one.
    sendOptionalReceipt(frame);
  }

  /**
   * Called when a {@link Command#SUBSCRIBE} frame is received from the client.
   * A new subscription will be setup and messages will immediately start being
   * consumed.
   * 
   * @param frame
   *          the frame to process
   * @throws JMSException
   * @throws IOException
   */
  private void onSubscribe(Frame frame) throws JMSException, IOException {
    // Get the destination and ID headers.
    String destName = getRequiredHeader("destination", frame);
    String id = getRequiredHeader("id", frame);

    // Check that this isn't an existing subscription ID.
    if (subscriptions.containsKey(id)) {
      throw new StompException(format(
          "Subscription with id [%s] already exists.", id));
    }

    // Create the JMS components.
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destination = config.getFrameConverter().fromFrameDestination(
        destName, session);
    MessageConsumer consumer = session.createConsumer(destination);

    // Create the subscription.
    ClientSubscription subscription = new ClientSubscription(messageCallback,
        id, consumer, session);
    subscriptions.put(id, subscription);

    // Send a receipt if the client asked for one.
    sendOptionalReceipt(frame);
  }

  /**
   * Called when a {@link Command#UNSUBSCRIBE} frame is received from the
   * client. If the subscription exists, it will be terminated.
   * 
   * @param frame
   *          the frame to process
   * @throws JMSException
   * @throws IOException
   */
  private void onUnsubscribe(Frame frame) throws JMSException, IOException {
    // Get the ID header.
    String id = getRequiredHeader("id", frame);

    // Lookup the existing subscription.
    ClientSubscription subscription = subscriptions.remove(id);

    // Check that it exists.
    if (subscription == null) {
      throw new StompException(format("Subscription with id [%s] not found.",
          id));
    }

    // Close the JMS components.
    safeClose(subscription.getConsumer());
    safeClose(subscription.getSession());

    // Send the receipt if the client asked.
    sendOptionalReceipt(frame);
  }

  /**
   * Returns the header with given name from the given frame. If the header
   * isn't found, a {@link StompException} will be raised.
   * 
   * @param name
   *          the header name
   * @param frame
   *          the frame from which to get the header
   * @return the header value
   * @throws StompException
   *           if the header isn't found
   */
  private String getRequiredHeader(String name, Frame frame)
      throws StompException {
    String value = frame.getHeaders().get(name);
    if (value == null) {
      throw new StompException(format("Header %s is required.", name));
    }
    return value;
  }

  /**
   * Called when a {@link Command#DISCONNECT} frame is received from the client.
   * This {@link ClientAgent} is closed and the thread will exit.
   * 
   * @param frame
   *          the frame to process
   * @throws IOException
   */
  private void onDisconnect(Frame frame) throws IOException {
    sendOptionalReceipt(frame);

    connected = false;
  }

  /**
   * Called when a {@link Command#CONNECT} frame is received from the client.
   * 
   * @param frame
   *          the frame to process
   * @throws IOException
   */
  private void onConnect(Frame frame) throws IOException {
    // TODO add version negotiation and heart-beat support.

    connected = true;

    // Send the response frame.
    FrameBuilder fb = FrameBuilder.command(Command.CONNECTED)
        .header("version", "1.2").headerContentTypeText();
    outstream.write(fb.build());

  }

  /**
   * Sends a receipt frame to the client if the request frame contains the
   * receipt header. Otherwise this method does nothing.
   * 
   * @param requestFrame
   *          the request frame for which to send a receipt
   * @throws IOException
   */
  private void sendOptionalReceipt(Frame requestFrame) throws IOException {
    if (requestFrame.getHeaders().containsKey("receipt")) {
      Frame response = new Frame(Command.RECEIPT);
      response.getHeaders().put("receipt-id",
          requestFrame.getHeaders().get("receipt"));
      outstream.write(response);
    }
  }

}
