package org.mpilone.hazelcastmq.core;

/**
 * A listener to be notified when a channel is read-ready and the receive
 * operation can be used to possibly get a message.
 *
 * @author mpilone
 */
public interface ReadReadyListener {

  /**
   * Called when a channel is read-ready. It is possible that the channel
   * doesn't have any pending messages because another consumer already received
   * the message.
   *
   * @param event the event details
   */
  void readReady(ReadReadyEvent event);

}
