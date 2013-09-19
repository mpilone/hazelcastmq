package org.mpilone.hazelcastmq.stomp.server;

import org.mpilone.hazelcastmq.core.*;

/**
 * An active subscription created by a STOMP client with the subscribe command.
 * 
 * @author mpilone
 */
class ClientSubscription implements HazelcastMQMessageListener {

  /**
   * The callback to be executed when a new message arrives.
   * 
   * @author mpilone
   */
  interface MessageCallback {
    /**
     * Called when a new message arrives on the subscription consumer. the
     * message should be delivered to the STOMP client.
     * 
     * @param subscription
     *          the original subscription information
     * @param msg
     *          the message that just arrived
     */
    public void onMessage(ClientSubscription subscription,
        HazelcastMQMessage msg);
  }

  /**
   * The session used to create the consumer.
   */
  private HazelcastMQContext context;

  /**
   * The consumer to receive messages for the subscription.
   */
  private HazelcastMQConsumer consumer;

  /**
   * The ID of the subscription.
   */
  private String subscriptionId;

  /**
   * The callback to handle the incoming messages.
   */
  private MessageCallback callback;

  /**
   * Constructs a subscription which will execute the given callback every time
   * a new message arrives.
   * 
   * @param callback
   *          the callback to executes
   * @param subscriptionId
   *          the ID of the subscription
   * @param consumer
   *          the consumer to receive messages
   * @param session
   *          the session that created the consumer
   */
  public ClientSubscription(MessageCallback callback, String subscriptionId,
      HazelcastMQConsumer consumer, HazelcastMQContext session) {
    super();
    this.callback = callback;
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

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
   */
  @Override
  public void onMessage(HazelcastMQMessage msg) {
    callback.onMessage(this, msg);
  }

}
