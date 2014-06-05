
package org.mpilone.hazelcastmq.stomp.server;

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.yeti.Frame;
import org.mpilone.yeti.FrameBuilder;
import org.mpilone.yeti.StompClientException;
import org.mpilone.yeti.server.ConnectDisconnectStomplet;

/**
 *
 * @author mpilone
 */
class HazelcastMQStomplet extends ConnectDisconnectStomplet {

  private final String DEFAULT_CONTEXT_TXN_ID =
      "hazelcastmq-stomp-server-default";

  /**
   * The configuration cached from the {@link #stomper} instance.
   */
  private final HazelcastMQStompConfig config;

  /**
   * The map of transaction ID to the transaction instance for all active client
   * transactions.
   */
  private final Map<String, ClientTransaction> transactions;

  /**
   * The map of subscription ID to the subscription instance for all active
   * client subscriptions.
   */
  private final Map<String, ClientSubscription> subscriptions;

  public HazelcastMQStomplet(HazelcastMQStompConfig config) {

    this.config = config;
    this.transactions = new HashMap<>();
    this.subscriptions = new HashMap<>();

    // Create the default context. This context will be used for all operations
    // outside of a named transaction.
    HazelcastMQContext mqContext = config.getHazelcastMQInstance()
        .createContext();
    mqContext.start();
    transactions.put(DEFAULT_CONTEXT_TXN_ID,
        new ClientTransaction(DEFAULT_CONTEXT_TXN_ID, mqContext));
  }

  @Override
  public void destroy() {
    // Close any open MQ contexts.
    for (ClientTransaction tx : transactions.values()) {
      safeClose(tx.getContext());
    }
    transactions.clear();

    // Close any open MQ subscriptions.
    for (ClientSubscription subscription : subscriptions.values()) {
      safeClose(subscription.getConsumer());
      safeClose(subscription.getContext());
    }
    subscriptions.clear();

    super.destroy();
  }

  @Override
  protected void doSubscribe(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the destination and ID headers.
    String destination = getRequiredHeader(
        org.mpilone.yeti.Headers.DESTINATION, frame);
    String id = getRequiredHeader(org.mpilone.yeti.Headers.ID, frame);

    // Check that this isn't an existing subscription ID.
    if (subscriptions.containsKey(id)) {
      throw new StompClientException(format(
          "Subscription with id [%s] already exists.", id));
    }

    // Create the JMS components.
    HazelcastMQContext session = config.getHazelcastMQInstance().createContext(
        false);
    HazelcastMQConsumer consumer = session.createConsumer(destination);

    // Create the subscription.
    ClientSubscription subscription = new ClientSubscription(id,
        res.getFrameChannel(), consumer, session);
    subscriptions.put(id, subscription);

    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doUnsubscribe(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the ID header.
    String id = getRequiredHeader("id", frame);

    // Lookup the existing subscription.
    ClientSubscription subscription = subscriptions.remove(id);

    // Check that it exists.
    if (subscription == null) {
      throw new StompClientException(format(
          "Subscription with id [%s] not found.",
          id));
    }

    // Close the MQ components.
    safeClose(subscription.getConsumer());
    safeClose(subscription.getContext());

    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doAbort(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.yeti.Headers.TRANSACTION, frame);

    // Make sure it exists.
    ClientTransaction tx = transactions.remove(transactionId);
    if (tx == null) {
      throw new StompClientException(format("Transaction [%s] is not active.",
          transactionId));
    }

    tx.getContext().rollback();
    safeClose(tx.getContext());

    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doCommit(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.yeti.Headers.TRANSACTION, frame);

    // Make sure it exists.
    ClientTransaction tx = transactions.remove(transactionId);
    if (tx == null) {
      throw new StompClientException(format("Transaction [%s] is not active.",
          transactionId));
    }

    tx.getContext().commit();
    safeClose(tx.getContext());

    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doBegin(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the transaction ID.
    String transactionId = getRequiredHeader(
        org.mpilone.yeti.Headers.TRANSACTION, frame);

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

    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doSend(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Get the destination header.
    String destName = getRequiredHeader(
        org.mpilone.yeti.Headers.DESTINATION, frame);

    // Get the optional transaction ID.
    String transactionId = frame.getHeaders().get(
        org.mpilone.yeti.Headers.TRANSACTION);

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

    writeOptionalReceipt(frame, res.getFrameChannel());
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
      throw new StompClientException("Required header not found.", format(
          "Header %s is required for the command %s.", name, frame.getCommand()),
          frame);
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

  /**
   * An active subscription created by a STOMP client with the subscribe
   * command.
   *
   * @author mpilone
   */
  class ClientSubscription implements HazelcastMQMessageListener {

    /**
     * The session used to create the consumer.
     */
    private final HazelcastMQContext context;

    /**
     * The consumer to receive messages for the subscription.
     */
    private final HazelcastMQConsumer consumer;

    /**
     * The ID of the subscription.
     */
    private final String subscriptionId;

    /**
     * The frame channel to write all messages for the subscription.
     */
    private final WritableFrameChannel frameChannel;

    /**
     * Constructs a subscription which will execute the given callback every
     * time a new message arrives.
     *
     * @param subscriptionId the ID of the subscription
     * @param frameChannel the frame channel to write all messages for the
     * subscription
     * @param consumer the consumer to receive messages
     * @param session the session that created the consumer
     */
    public ClientSubscription(String subscriptionId,
        WritableFrameChannel frameChannel,
        HazelcastMQConsumer consumer, HazelcastMQContext session) {
      super();
      this.subscriptionId = subscriptionId;
      this.consumer = consumer;
      this.context = session;
      this.frameChannel = frameChannel;

      consumer.setMessageListener(this);
    }

    /**
     * @return the session
     */
    public HazelcastMQContext getContext() {
      return context;
    }

    /**
     * @return the consumer
     */
    public HazelcastMQConsumer getConsumer() {
      return consumer;
    }

    /**
     * @return the subscriptionId
     */
    public String getSubscriptionId() {
      return subscriptionId;
    }

    @Override
    public void onMessage(HazelcastMQMessage msg) {
      FrameBuilder fb = FrameBuilder.copy(config.getFrameConverter().
          toFrame(msg));
      fb.header(org.mpilone.yeti.Headers.SUBSCRIPTION, getSubscriptionId());

      if (frameChannel != null) {
        frameChannel.write(fb.build());
      }
    }

  }

}
