package org.mpilone.hazelcastmq.stomper;

import javax.jms.*;

/**
 * An active subscription created by a STOMP client with the subscribe command.
 * 
 * @author mpilone
 */
class ClientSubscription implements MessageListener {

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
    public void onMessage(ClientSubscription subscription, Message msg);
  }

  /**
   * The session used to create the consumer.
   */
  private Session session;

  /**
   * The consumer to receive messages for the subscription.
   */
  private MessageConsumer consumer;

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
      MessageConsumer consumer, Session session) throws JMSException {
    super();
    this.callback = callback;
    this.subscriptionId = subscriptionId;
    this.consumer = consumer;
    this.session = session;

    consumer.setMessageListener(this);
  }

  /**
   * @return the session
   */
  public Session getSession() {
    return session;
  }

  /**
   * @return the consumer
   */
  public MessageConsumer getConsumer() {
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
  public void onMessage(Message msg) {
    callback.onMessage(this, msg);
  }

}
