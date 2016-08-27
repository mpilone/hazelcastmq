package org.mpilone.hazelcastmq.core;

/**
 * The message acknowledgment mode of a channel.
 *
 * @author mpilone
 */
public enum AckMode {

  /**
   * Messages are automatically acknowledged as soon as they are read from the
   * channel. The client should not attempt to manually acknowledge messages.
   */
  AUTO,
  /**
   * Messages that are read from the channel are assumed to be "in-flight" and
   * must be individually acknowledged by the client/consumer. If a message is
   * not acknowledged, it may be placed back into the channel for redelivery
   * after a configured timeout. Each message must be acked/nacked individually
   * and no cumulative acknowledgment is supported.
   */
  CLIENT_INDIVIDUAL,
  /**
   * Messages that are read from the channel are assumed to be "in-flight" and
   * must be individually acknowledged by the client/consumer. If a message is
   * not acknowledged, it may be placed back into the channel for redelivery
   * after a configured timeout. All messages received up to and including the
   * acked/nacked message will be included in the ack/nack. Unlike
   * {@link #CLIENT_INDIVIDUAL}, this mode allows the client to perform multiple
   * receive calls and then just acknowledge the last message.
   */
  CLIENT

}
