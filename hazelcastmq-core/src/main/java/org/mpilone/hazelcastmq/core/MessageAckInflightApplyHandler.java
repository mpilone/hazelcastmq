package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.Serializable;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.mpilone.hazelcastmq.core.MessageAckInflightAdapter.MESSAGE_ACK_QUEUE_NAME;

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
class MessageAckInflightApplyHandler extends MessageAckInflightAdapter {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      MessageAckInflightApplyHandler.class);

  /**
   * The period between checks of the inflight map where inflight messages that
   * have timed out will be automatically nacked.
   */
  private final static long AUTO_NACK_PERIOD = TimeUnit.SECONDS.toMillis(
      30);
  private final Object autoAckMutex = new Object();

  private final String brokerName;
  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;

  private long lastAutoNackTime;
  private Clock clock = Clock.systemUTC();

  /**
   * Constructs the resender.
   *
   * @param config the broker configuration
   */
  public MessageAckInflightApplyHandler(String brokerName,
      BrokerConfig config) {

    this.brokerName = brokerName;
    this.config = config;
    this.hazelcastInstance = config.getHazelcastInstance();
  }

  @Override
  protected void messageInflight(MessageInflight inflight) {
    maybeAutoNack();
  }

  @Override
  protected void messageAck(MessageAck ack) {

    final IExecutorService executor = hazelcastInstance.getExecutorService(
        config.getRouterExecutorName());
    final Member member = hazelcastInstance.getCluster().getLocalMember();

    executor.executeOnMember(new ApplyAckTask(brokerName), member);

    maybeAutoNack();
  }

  /**
   * Runs the auto-nack check the {@link #AUTO_NACK_PERIOD} has expired. Any
   * inflight messages that have timed out will be automatically nacked.
   */
  private void maybeAutoNack() {

    synchronized (autoAckMutex) {

      long now = clock.millis();
      if (now - lastAutoNackTime > AUTO_NACK_PERIOD) {

      // Auto ack period expired. Check for any expired inflight messages
        // and nack them.
        lastAutoNackTime = now;

        final IExecutorService executor = hazelcastInstance.getExecutorService(
            "hzmq.ackexecutor");
        final Member member = hazelcastInstance.getCluster().getLocalMember();
        final long timeout = TimeUnit.SECONDS.toMillis(config
            .getInflightTimeout());

        log.fine("Submitting auto nack task for inflight messages.");

        executor.executeOnMember(new AutoNackTask(brokerName, timeout, now),
            member);
      }
    }
  }

  private static class AutoNackTask implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerName;
    private final long timeout;
    private final long now;

    public AutoNackTask(String brokerName, long timeout, long now) {
      this.brokerName = brokerName;
      this.timeout = timeout;
      this.now = now;
    }

    @Override
    public void run() {

      final Broker broker = HazelcastMQ.getBrokerByName(brokerName);

      if (broker == null) {
        // Log and return.
        log.warning(format("Unable to find broker %s to auto nack "
            + "inflight messages.", brokerName));

        return;
      }

      final HazelcastInstance hazelcastInstance = broker.getConfig()
          .getHazelcastInstance();

      final IQueue<MessageAck> ackQueue = hazelcastInstance.getQueue(
          MESSAGE_ACK_QUEUE_NAME);
      final IMap<String, MessageInflight> inflightMap = hazelcastInstance
          .getMap(MESSAGE_INFLIGHT_MAP_NAME);

      inflightMap.entrySet(entry -> {

        // Find the entries that have expired.
        final MessageInflight inflight = (MessageInflight) entry.getValue();
        return now - inflight.getTimestamp() > timeout;
      }).stream().map(Map.Entry::getKey).forEach(msgId -> {

        // Remove the entry from the map.
        final MessageInflight inflight = inflightMap.remove(msgId);

        // Check to make sure some other thread didn't already apply the nack.
        if (inflight != null) {
          log.fine(format("Generating a nack for message %s because the "
              + "inflight message has timed out.", msgId));

          ackQueue.offer(new MessageAck(msgId, true));
        }

      });
    }
  }

  private static class ApplyAckTask implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerName;

    public ApplyAckTask(String brokerName) {
      this.brokerName = brokerName;
    }

    @Override
    public void run() {

      final Broker broker = HazelcastMQ.getBrokerByName(brokerName);

      if (broker == null) {
        // Log and return.
        log.warning(
            format("Unable to find broker %s to apply acks.", brokerName));

        return;
      }

      final HazelcastInstance hazelcastInstance = broker.getConfig()
          .getHazelcastInstance();

      final IQueue<MessageAck> ackQueue = hazelcastInstance.getQueue(
          MESSAGE_ACK_QUEUE_NAME);
      final IMap<String, MessageInflight> inflightMap = hazelcastInstance
          .getMap(
              MESSAGE_INFLIGHT_MAP_NAME);
      try (ChannelContext channelContext = broker.createChannelContext()) {

        MessageAck ack;
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
    }
  }
}
