package org.mpilone.hazelcastmq.core;

/**
 * <p>
 * A client uses a message listener to receive messages asynchronously from a
 * destination by registering the listener on a {@link HazelcastMQConsumer}.</p>
 * <p>
 * Each context must ensure that it passes messages serially to the listener.
 * This means that a listener assigned to one or more consumers of the same
 * context can assume that the onMessage method is not called with the next
 * message until the session has completed the last call.
 * </p>
 *
 * @author mpilone
 */
public interface HazelcastMQMessageListener {

  /**
   * Passes a message to the listener
   *
   * @param msg the message passed to the listener
   */
  void onMessage(HazelcastMQMessage msg);
}
