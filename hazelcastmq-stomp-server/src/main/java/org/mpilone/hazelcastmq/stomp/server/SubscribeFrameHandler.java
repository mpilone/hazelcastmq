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
 class SubscribeFrameHandler extends SimpleChannelInboundHandler<Frame> {

  /**
   * The configuration cached from the {@link #stomper} instance.
   */
  private final HazelcastMQStompServerConfig config;

  /**
   * The map of subscription ID to the subscription instance for all active
   * client subscriptions.
   */
  private final Map<String, ClientSubscription> subscriptions;

  private Channel channel;

  public SubscribeFrameHandler(HazelcastMQStompServerConfig config) {
    super(Frame.class, true);

    this.config = config;
    this.subscriptions = new HashMap<String, ClientSubscription>();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    switch (frame.getCommand()) {
      case SUBSCRIBE:
        onSubscribe(frame);
        break;

      case UNSUBSCRIBE:
        onUnsubscribe(frame);
        break;
    }

    ctx.fireChannelRead(frame);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    this.channel = ctx.channel();

    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // Close any open MQ subscriptions.
    for (ClientSubscription subscription : subscriptions.values()) {
      safeClose(subscription.getConsumer());
      safeClose(subscription.getContext());
    }
    subscriptions.clear();

    this.channel = null;

    System.out.println("SubscribeFrameHandler: Channel inactive.");
    super.channelInactive(ctx);
  }

  /**
   * Called when a {@link Command#SUBSCRIBE} frame is received from the client.
   * A new subscription will be setup and messages will immediately start being
   * consumed.
   *
   * @param frame the frame to process
   *
   * @throws IOException
   */
  private void onSubscribe(Frame frame) throws IOException {
    // Get the destination and ID headers.
    String destination = getRequiredHeader(
        org.mpilone.stomp.Headers.DESTINATION, frame);
    String id = getRequiredHeader(org.mpilone.stomp.Headers.ID, frame);

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
    ClientSubscription subscription = new ClientSubscription(id, consumer,
        session);
    subscriptions.put(id, subscription);
  }

  /**
   * Called when a {@link Command#UNSUBSCRIBE} frame is received from the
   * client. If the subscription exists, it will be terminated.
   *
   * @param frame the frame to process
   *
   * @throws IOException
   */
  private void onUnsubscribe(Frame frame) throws IOException {
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
     * Constructs a subscription which will execute the given callback every
     * time a new message arrives.
     *
     * @param callback the callback to executes
     * @param subscriptionId the ID of the subscription
     * @param consumer the consumer to receive messages
     * @param session the session that created the consumer
     */
    public ClientSubscription(String subscriptionId,
        HazelcastMQConsumer consumer, HazelcastMQContext session) {
      super();
      this.subscriptionId = subscriptionId;
      this.consumer = consumer;
      this.context = session;

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
      Frame frame = config.getFrameConverter().toFrame(msg);
      frame.getHeaders()
          .put(org.mpilone.stomp.Headers.SUBSCRIPTION,
              getSubscriptionId());

      if (channel != null) {
        channel.writeAndFlush(frame);
      }
    }

  }
}
