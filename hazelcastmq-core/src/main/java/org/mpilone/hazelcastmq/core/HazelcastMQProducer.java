package org.mpilone.hazelcastmq.core;

/**
 * <p>
 * A client uses a message producer to send messages to a destination. A message
 * producer is created using a {@link HazelcastMQContext}.
 * </p>
 * <p>
 * A client also has the option of creating a message producer without supplying
 * a destination. In this case, a destination must be provided with every send
 * operation. A typical use for this kind of message producer is to send replies
 * to requests using the request's reply-to destination.
 * </p>
 * <p>
 * A client can specify a default delivery mode, priority, time to live and
 * delivery delay for messages sent by a message producer. It can also specify
 * the delivery mode, priority, and time to live for an individual message.
 * </p><p>
 * A client can specify a time-to-live value in milliseconds for each message it
 * sends. This value defines a message expiration time that is the sum of the
 * message's time-to-live and the GMT when it is sent (for transacted sends,
 * this is the time the client sends the message, not the time the transaction
 * is committed).
 * </p>
 * <p>
 * HazelcastMQ should do its best to expire messages accurately; however, the
 * JMS API does not define the accuracy provided.
 * </p>
 *
 * @author mpilone
 */
public interface HazelcastMQProducer {

  /**
   * Sends a message using the producer's destination and default time to live.
   * This method can only be used if a destination was specified at producer
   * creation time.
   *
   * @param body the body of the message
   */
  void send(byte[] body);

  /**
   * Sends a message using the producer's destination and default time to live.
   * This method can only be used if a destination was specified at producer
   * creation time.
   *
   * @param body the body of the message
   */
  void send(String body);

  /**
   * Sends a message using the producer's destination and default time to live.
   * This method can only be used if a destination was specified at producer
   * creation time.
   *
   * @param msg the message to send
   */
  void send(HazelcastMQMessage msg);

  /**
   * Sends a message using the producer's destination and the given time to
   * live. This method can only be used if a destination was specified at
   * producer creation time.
   *
   * @param timeToLive the message's lifetime in milliseconds (0 to never
   * expire)
   * @param msg the message to send
   */
  void send(HazelcastMQMessage msg, long timeToLive);

  /**
   * Sends a message using the given destination and default time to live. This
   * method can only be used if a destination was not specified at producer
   * creation time.
   *
   * @param destination the destination to send the message to
   * @param body the body of the message
   */
   void send(String destination, byte[] body);

  /**
   * Sends a message using the given destination and default time to live. This
   * method can only be used if a destination was not specified at producer
   * creation time.
   *
   * @param destination the destination to send the message to
   * @param body the body of the message
   */
   void send(String destination, String body);

  /**
   * Sends a message using the given destination and default time to live. This
   * method can only be used if a destination was not specified at producer
   * creation time.
   *
   * @param destination the destination to send the message to
   * @param msg the message to send
   */
  void send(String destination, HazelcastMQMessage msg);

  /**
   * Sends a message using the given destination and the given time to live.
   * This method can only be used if a destination was not specified at producer
   * creation time.
   *
   * @param destination the destination to send the message to
   * @param msg the message to send
   * @param timeToLive the message's lifetime in milliseconds (0 to never
   * expire)
   */
  void send(String destination, HazelcastMQMessage msg, long timeToLive);

  /**
   * Sets the time to live value used as the default for all messages sent
   * by this producer unless the value is specifically given in the send method.
   *
   * @param timeToLive the message's lifetime in milliseconds (0 to never
   * expire)
   */
  void setTimeToLive(long timeToLive);

  /**
   * Returns the time to live value used as the default for all messages sent by
   * this producer.
   *
   * @return the message's lifetime in milliseconds
   */
  long getTimeToLive();
}
