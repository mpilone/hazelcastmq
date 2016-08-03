package org.mpilone.hazelcastmq.stomp;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.yeti.*;
import org.mpilone.yeti.server.ConnectDisconnectStomplet;

/**
 * A {@link Stomplet} implementation that map subscribe and send functionality
 * to a {@link HazelcastMQInstance}.
 *
 * @author mpilone
 */
class StompAdapterStomplet extends ConnectDisconnectStomplet {

  private final static String DEFAULT_CONTEXT_TXN_ID =
      "hazelcastmq-stomp-server-default";

  /**
   * The configuration cached from the {@link #stomper} instance.
   */
  private final StompAdapterConfig config;

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

  /**
   * Constructs the stomplet with the given configuration.
   *
   * @param config the stomplet configuration
   */
  public StompAdapterStomplet(StompAdapterConfig config) {

    super(new StompVersion[]{StompVersion.VERSION_1_1, StompVersion.VERSION_1_2});

    this.config = config;
    this.transactions = new HashMap<>();
    this.subscriptions = new HashMap<>();

    // Create the default transaction. This context will be used for all
    // operations outside of a named transaction.
    transactions.put(DEFAULT_CONTEXT_TXN_ID,
        new ClientTransaction(DEFAULT_CONTEXT_TXN_ID, config.getBroker()));
  }

  @Override
  protected void postConnect(StompVersion version, Frame frame,
      FrameBuilder connectedFrameBuilder) {
    super.postConnect(version, frame, connectedFrameBuilder);

    connectedFrameBuilder.header(org.mpilone.yeti.Headers.SERVER,
        "HazelcastMQ");
  }

  @Override
  public void destroy() {
    // Close any open MQ contexts.
    transactions.values().forEach(ClientTransaction::close);
    transactions.clear();

    // Close any open MQ subscriptions.
    subscriptions.values().forEach(ClientSubscription::close);
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
          "Subscription with id %s already exists.", id), null, frame);
    }

    // Create the subscription.
    ClientSubscription subscription = new ClientSubscription(id,
        res.getFrameChannel(), config.getBroker(), DataStructureKey.fromString(
            destination), config.getExecutor());
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
          "Subscription with id %s not found.", id), null, frame);
    }

    // Close the MQ components.
    subscription.close();

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
      throw new StompClientException(format("Transaction %s is not active.",
          transactionId), null, frame);
    }

    tx.getContext().rollback();
    tx.close();

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
      throw new StompClientException(format("Transaction %s is not active.",
          transactionId), null, frame);
    }

    tx.getContext().commit();
    tx.close();

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
          "Transaction %s is already active.",
          transactionId), null, frame);
    }

    // Create a transacted session and store it away for future commands.
    ChannelContext mqContext = config.getBroker().createChannelContext();
    mqContext.setAutoCommit(false);
    transactions.put(transactionId, new ClientTransaction(transactionId,
        config.getBroker()));

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
      throw new StompClientException(format("Transaction %s is not active.",
          transactionId), null, frame);
    }
    ChannelContext mqContext = tx.getContext();

    // Create the producer.
    Channel mqChannel = mqContext.createChannel(DataStructureKey.
        fromString(destName));

    // Convert and send the message.
    mqChannel.send(config.getFrameConverter().fromFrame(frame));

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
      throw new StompClientException(format(
          "Header %s is required for the command %s.", name,
          frame.getCommand()), null, frame);
    }
    return value;
  }

  /**
   * An active transaction for a client.
   *
   * @author mpilone
   */
  static class ClientTransaction implements AutoCloseable {

    /**
     * The unique ID of the transaction.
     */
    private final String transactionId;

    /**
     * The {@link ChannelContext} used for all message production within the
     * transaction.
     */
    private final ChannelContext mqContext;

    /**
     * Constructs the transaction.
     *
     * @param transactionId the unique ID of the transaction
     * @param context the context used for all message production within the transaction
     */
    public ClientTransaction(String transactionId, Broker broker) {
      super();
      this.transactionId = transactionId;
      this.mqContext = broker.createChannelContext();
      mqContext.setAutoCommit(transactionId.equals(DEFAULT_CONTEXT_TXN_ID));
    }

    /**
     * Returns the unique ID of the transaction.
     *
     * @return the ID of the transaction
     */
    public String getTransactionId() {
      return transactionId;
    }

    /**
     * Returns the channel context for this transaction.
     *
     * @return the channel context
     */
    public ChannelContext getContext() {
      return mqContext;
    }

    @Override
    public void close() {
      mqContext.close();
    }

  }

  /**
   * An active subscription created by a STOMP client with the subscribe
   * command.
   *
   * @author mpilone
   */
  class ClientSubscription implements MessageDispatcher.Performer, AutoCloseable {

    /**
     * The ID of the subscription.
     */
    private final String subscriptionId;

    /**
     * The frame channel to write all messages for the subscription.
     */
    private final WritableFrameChannel frameChannel;

    private final SingleThreadPollingMessageDispatcher dispatcher;

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
        Broker broker, DataStructureKey channelKey, ExecutorService executor) {
      super();
      this.subscriptionId = subscriptionId;
      this.frameChannel = frameChannel;

      this.dispatcher = new SingleThreadPollingMessageDispatcher();
      dispatcher.setChannelKey(channelKey);
      dispatcher.setBroker(broker);
      dispatcher.setExecutor(executor);
      dispatcher.setPerformer(this);
      dispatcher.start();
    }

    @Override
    public void close() {
      dispatcher.stop();
    }

    /**
     * Returns the unique subscription ID.
     *
     * @return the subscription ID
     */
    public String getSubscriptionId() {
      return subscriptionId;
    }

    @Override
    public void perform(
        Message<?> msg) {
      FrameBuilder fb = FrameBuilder.copy(config.getFrameConverter().
          toFrame(msg));
      fb.header(org.mpilone.yeti.Headers.SUBSCRIPTION, getSubscriptionId());

      if (frameChannel != null) {
        frameChannel.write(fb.build());
      }
    }
  }
}
