package org.mpilone.hazelcastmq.core;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A message consumer that will receive messages from a single destination. A
 * consumer must be started by calling {@link HazelcastMQContext#start()} in
 * order to start receiving messages. A consumer will be automatically started if the
 * {@link HazelcastMQContext} is set to auto start. Messages can be "pushed" by
 * registering a {@link HazelcastMQMessageListener} or they can be "pulled"
 * using the {@link #receive()} methods.
 * </p>
 * <p>
 * A consumer is single-threaded and NOT thread-safe. If a message listener is
 * registered, the control thread for the consumer is the in the context that
 * created the consumer. Therefore it is only safe to call the {@link #close() }
 * and {@link #setMessageListener(org.mpilone.hazelcastmq.core.HazelcastMQMessageListener)
 * }.
  * </p>
 *
 * @author mpilone
 */
public interface HazelcastMQConsumer extends Closeable {

  /**
   * <p>
   * Closes the consumer.
   * </p>
   * <p>
   * Since a provider may allocate some resources on behalf of a consumer
   * outside the Java virtual machine, clients should close them when they are
   * not needed. Relying on garbage collection to eventually reclaim these
   * resources may not be timely enough.
   * </p>
   * <p>
   * This call will block until a receive call in progress on this consumer has
   * completed. A blocked receive call returns null when this consumer is
   * closed.
   * </p>
   * <p>
   * If this method is called whilst a message listener is in progress in
   * another thread then it will block until the message listener has completed.
   * </p>
   * <p>
   * This method may be called from a message listener's onMessage method on its
   * own consumer. After this method returns the onMessage method will be
   * allowed to complete normally.
   * </p>
   * <p>
   * This method is the only consumer method that can be called concurrently.
   * </p>
   */
  @Override
  public void close();

  /**
   * Returns the registered message listener or null if there is no message
   * listener set.
   *
   * @return the message listener or null
   */
   HazelcastMQMessageListener getMessageListener();

  /**
   * Receives the next message and blocks until a message becomes available or
   * until the consumer is closed.
   *
   * @return the message received or null if the consumer was closed
   */
   HazelcastMQMessage receive();

  /**
   * Receives the next message and blocks until a message becomes available, the
   * timeout expires, or the consumer is closed.
   *
   * @param timeout the amount of time to block for a message
   * @param unit the unit of the time value
   *
   * @return the message received or null if the timeout expired or the consumer
   * was closed
   */
   HazelcastMQMessage receive(long timeout, TimeUnit unit);

  /**
   * Receives the next message, returning immediately if no message is
   * available.
   *
   * @return the next message or null if no message was available
   */
   HazelcastMQMessage receiveNoWait();

  /**
   * A convenience method that calls
   * {@link #receive(long, java.util.concurrent.TimeUnit)} and then extracts the
   * body of the message if a message was received.
   *
   * @param timeout the amount of time to block for a message
   * @param unit the unit of the time value
   *
   * @return the body of the message received or null if the timeout expired or
   * the consumer was closed
   */
   byte[] receiveBody(long timeout, TimeUnit unit);

  /**
   * A convenience method that calls {@link #receiveNoWait()} and then extracts
   * the body of the message if a message was received.
   *
   * @return the body of the message received or null no message was received or
   * the consumer was closed
   */
   byte[] receiveBodyNoWait();

  /**
   * <p>
   * Sets the consumer's message listener.
   * </p>
   * <p>
   * Setting the MessageListener to null is the equivalent of unsetting the
   * message listener for the consumer.
   * </p>
   * <p>
   * The effect of calling this method while messages are being consumed by an
   * existing listener or the consumer is being used to consume messages
   * synchronously is undefined. It is a client programming error for a
   * MessageListener to throw an exception.
   * </p>
   *
   * @param listener the listener to set or null to clear the current listener
   */
   void setMessageListener(HazelcastMQMessageListener listener);

}
