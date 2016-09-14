package org.mpilone.hazelcastmq.core;

/**
 * A context for marking a message as inflight.
 *
 * @author mpilone
 */
interface InflightContext {

  /**
   * Marks the given message as inflight on the given channel. Depending on the
   * ack mode and transaction status, the message may be placed into inflight
   * status pending an ack or nack from the client.
   *
   * @param channelKey the channel the message is inflight on
   * @param message the message that is inflight
   */
  void inflight(DataStructureKey channelKey, Message<?> message);
}
