package org.mpilone.hazelcastmq.stomp.server;

import org.mpilone.stomp.StompClientException;
import org.mpilone.stomp.Frame;

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.mpilone.hazelcastmq.core.*;

import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
 class SendFrameHandler extends SimpleChannelInboundHandler<Frame> {

  private final String DEFAULT_CONTEXT_TXN_ID =
      "hazelcastmq-stomp-server-default";

  /**
   * The configuration cached from the {@link #stomper} instance.
   */
  private final HazelcastMQStompServerConfig config;

  /**
   * The map of transaction ID to the transaction instance for all active client
   * transactions.
   */
  private final Map<String, ClientTransaction> transactions;

  public SendFrameHandler(HazelcastMQStompServerConfig config) {
    super(Frame.class, true);

    this.config = config;
    this.transactions = new HashMap<String, ClientTransaction>();

    // Create the default context. This context will be used for all operations
    // outside of a named transaction.
    HazelcastMQContext mqContext = config.getHazelcastMQInstance()
        .createContext();
    mqContext.start();
    transactions.put(DEFAULT_CONTEXT_TXN_ID, new ClientTransaction(
        DEFAULT_CONTEXT_TXN_ID, mqContext));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    switch (frame.getCommand()) {
      case SEND:
        onSend(frame);
        break;

      case BEGIN:
        onBegin(frame);
        break;

      case ABORT:
        onAbort(frame);
        break;

      case COMMIT:
        onCommit(frame);
        break;

      default:
        ctx.fireChannelRead(frame);
        break;
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // Close any open MQ contexts.
    for (ClientTransaction tx : transactions.values()) {
      safeClose(tx.getContext());
    }
    transactions.clear();

    System.out.println("SendFrameHandler: Channel inactive.");
    super.channelInactive(ctx);
  }

  /**
   * Called when a {@link Command#ABORT} frame is received from the client. The
   * session associated with the transaction will be rolled back and closed.
   *
   * @param frame the frame to process
   *
   * @throws IOException
   */
  private void onAbort(Frame frame) throws IOException {

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.stomp.Headers.TRANSACTION, frame);

    // Make sure it exists.
    ClientTransaction tx = transactions.remove(transactionId);
    if (tx == null) {
      throw new StompClientException(format("Transaction [%s] is not active.",
          transactionId));
    }

    tx.getContext().rollback();
    safeClose(tx.getContext());
  }

  /**
   * Called when a {@link Command#COMMIT} frame is received from the client. The
   * session associated with the transaction will be committed and closed.
   *
   * @param frame the frame to process
   *
   * @throws JMSException
   * @throws IOException
   */
  private void onCommit(Frame frame) throws IOException {

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.stomp.Headers.TRANSACTION, frame);

    // Make sure it exists.
    ClientTransaction tx = transactions.remove(transactionId);
    if (tx == null) {
      throw new StompClientException(format("Transaction [%s] is not active.",
          transactionId));
    }

    tx.getContext().commit();
    safeClose(tx.getContext());
  }

  /**
   * Called when a {@link Command#BEGIN} frame is received from the client. A
   * new transacted session will be created and stored for future commands.
   *
   * @param frame the frame to process
   *
   * @throws IOException
   */
  private void onBegin(Frame frame) throws IOException {

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.stomp.Headers.TRANSACTION, frame);

    // Make sure it isn't already in use.
    if (transactions.containsKey(transactionId)) {
      throw new StompClientException(format(
          "Transaction [%s] is already active.",
          transactionId));
    }

    // Create a transacted session and store it away for future commands.
    HazelcastMQContext mqContext = config.getHazelcastMQInstance().
        createContext(
            true);
    transactions.put(transactionId, new ClientTransaction(transactionId,
        mqContext));
  }

  /**
   * Called when a {@link Command#SEND} frame is received from the client. The
   * frame will be converted to a JMS {@link Message} and dispatched to the
   * appropriate destination.
   *
   * @param frame the frame to process
   *
   * @throws IOException
   */
  private void onSend(Frame frame) throws IOException {

    // Get the destination header.
    String destName = getRequiredHeader(
        org.mpilone.stomp.Headers.DESTINATION, frame);

    // Get the optional transaction ID.
    String transactionId = frame.getHeaders().get(
        org.mpilone.stomp.Headers.TRANSACTION);

    // Use a transacted context if we have an ID. Otherwise use the default
    // context.
    if (transactionId == null) {
      transactionId = DEFAULT_CONTEXT_TXN_ID;
    }

    ClientTransaction tx = transactions.get(transactionId);
    if (tx == null) {
      throw new StompClientException(format("Transaction [%s] is not active.",
          transactionId));
    }
    HazelcastMQContext context = tx.getContext();

    // Create the producer.
    HazelcastMQProducer producer = context.createProducer();

    // Convert and send the message.
    producer.send(destName, config.getFrameConverter().fromFrame(frame));
  }

  /**
   * Returns the header with given name from the given frame. If the header
   * isn't found, a {@link StompClientException} will be raised.
   *
   * @param name the header name
   * @param frame the frame from which to get the header
   *
   * @return the header value
   * @throws StompClientException if the header isn't found
   */
  private String getRequiredHeader(String name, Frame frame)
      throws StompClientException {
    String value = frame.getHeaders().get(name);
    if (value == null) {
      throw new StompClientException(format("Header %s is required.", name));
    }
    return value;
  }

  /**
   * Closes the given instance, ignoring any exceptions.
   *
   * @param closeable the closeable to close
   */
  private static void safeClose(Closeable closeable) {
    try {
      closeable.close();
    }
    catch (IOException ex) {
      // Ignore
    }
  }

  /**
   * An active transaction for a client.
   *
   * @author mpilone
   */
  static class ClientTransaction {

    /**
     * The unique ID of the transaction.
     */
    private final String transactionId;

    /**
     * The {@link HazelcastMQContext} used for all message production within the
     * transaction.
     */
    private final HazelcastMQContext context;

    /**
     * Constructs the transaction.
     *
     * @param transactionId the unique ID of the transaction
     * @param context the HazelcastMQContext used for all message production
     * within the transaction
     */
    public ClientTransaction(String transactionId, HazelcastMQContext context) {
      super();
      this.transactionId = transactionId;
      this.context = context;
    }

    /**
     * @return the id
     */
    public String getTransactionId() {
      return transactionId;
    }

    /**
     * @return the session
     */
    public HazelcastMQContext getContext() {
      return context;
    }

  }
}
