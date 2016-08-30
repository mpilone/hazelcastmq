package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * An ack/inflight listener that will resend nacked messages on the original
 * message channel. Periodically the resender will generated nacks
 * for inflight messages that have timed out based on the {@link BrokerConfig#getInflightTimeout()
 * }. The resender should be added to the inflight map and the ack/nack queue as
 * a listener. To avoid using an additional thread, the resender operates in the
 * map/queue event thread and will only nack expired inflight messages when
 * there is inflight message activity.
 *
 * @author mpilone
 */
class MessageAckInflightResender extends MessageAckInflightAdapter {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      MessageAckInflightResender.class);

  /**
   * The period between checks of the inflight map where inflight messages that
   * have timed out will be automatically nacked.
   */
  private final static long AUTO_NACK_PERIOD = TimeUnit.SECONDS.toMillis(
      30);
  private final Object autoAckMutex = new Object();

  private final BrokerConfig brokerConfig;
  private final HazelcastInstance hazelcastInstance;

  private long lastAutoNackTime;
  private Clock clock = Clock.systemUTC();

  /**
   * Constructs the resender.
   *
   * @param brokerConfig the broker configuration
   */
  public MessageAckInflightResender(BrokerConfig brokerConfig) {
    this.brokerConfig = brokerConfig;
    this.hazelcastInstance = brokerConfig.getHazelcastInstance();
  }

  @Override
  protected void messageInflight(MessageInflight inflight) {
    autoNack();
  }

  @Override
  protected void messageAck(MessageAck ack) {

    final IQueue<MessageAck> ackQueue = hazelcastInstance.getQueue(
        MESSAGE_ACK_QUEUE_NAME);
    final IMap<String, MessageInflight> inflightMap = hazelcastInstance.getMap(
        MESSAGE_INFLIGHT_MAP_NAME);
    try (final DefaultChannelContext channelContext = new DefaultChannelContext(
        child -> {
        }, brokerConfig)) {

      while ((ack = ackQueue.poll()) != null) {

        // See if the message is still inflight.
        final String msgId = ack.getMessageId();
        final MessageInflight inflight = inflightMap.remove(msgId);

        if (inflight == null) {
          log.fine(format("Ignoring ack/nack for message %s because it "
              + "is not inflight.", msgId));
        } else if (ack.isNegative()) {

          // Perform the nack by resending the message.
          log.fine(format(
              "Applying nack for message %s. It will be resent to channel %s.",
              msgId, inflight.getChannelKey()));

          try (Channel channel = channelContext.createChannel(inflight
              .getChannelKey())) {

            // TODO: it would be nice to have a resend counter here and drop
            // messages (or send to DLQ) when the resend count is too high.
            channel.send(inflight.getMessage());
          }
        } else {

          // The ack is done because we removed the inflight message from
          // the map.
          log.fine(format("Applying ack for message %s.", msgId));
        }
      }
    }

    autoNack();
  }

  /**
   * Runs the auto-nack check the {@link #AUTO_NACK_PERIOD} has expired. Any
   * inflight messages that have timed out will be automatically nacked.
   */
  private void autoNack() {

    synchronized (autoAckMutex) {

      long now = clock.millis();
      if (now - lastAutoNackTime <= AUTO_NACK_PERIOD) {

        // Under the auto nack period. Nothing to do.
        return;
      }

      // Auto ack period expired. Check for any expired inflight messages
      // and nack them.
      lastAutoNackTime = now;

      log.fine("Running auto nack on inflight messages.");

      final IQueue<MessageAck> ackQueue = hazelcastInstance.getQueue(
          MESSAGE_ACK_QUEUE_NAME);
      final IMap<String, MessageInflight> inflightMap = hazelcastInstance
          .getMap(MESSAGE_INFLIGHT_MAP_NAME);
      final long timeout = TimeUnit.SECONDS.toMillis(brokerConfig
          .getInflightTimeout());

      inflightMap.values().forEach(inflight -> {

        if (now - inflight.getTimestamp() > timeout) {
          final String msgId = inflight.getMessage().getHeaders().getId();

          log.fine(format("Generating a nack for message %s because the "
              + "inflight message has timed out.", msgId));

          ackQueue.offer(new MessageAck(msgId, true));
        }
      });
    }
  }

}
