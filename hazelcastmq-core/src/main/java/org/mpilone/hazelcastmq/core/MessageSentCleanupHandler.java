
package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.Serializable;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 *
 * @author mpilone
 */
class MessageSentCleanupHandler extends MessageSentAdapter {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(QueueChannel.class);

  /**
   * The amount of time before an entry in the message sent map is considered
   * expired and will be removed. Normally there is one entry per channel so the
   * map reaches a steady state but it is possible to have a lot of entries for
   * temporary channels that can be removed periodically to release resources.
   */
  private static final long MESSAGE_SENT_EXPIRATION = TimeUnit.MINUTES.toMillis(
      2);

  private final String brokerName;
  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;

  private Clock clock = Clock.systemUTC();
  private long lastRunTime = 0;

  public MessageSentCleanupHandler(String brokerName, BrokerConfig config) {
    this.config = config;
    this.brokerName = brokerName;
    this.hazelcastInstance = config.getHazelcastInstance();
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    final long now = clock.millis();
    if (now - lastRunTime > MESSAGE_SENT_EXPIRATION) {
      lastRunTime = now;

      final IExecutorService executor = hazelcastInstance.getExecutorService(
          config.getRouterExecutorName());
      final Member member = hazelcastInstance.getCluster().getLocalMember();

      executor.executeOnMember(new ExpireSentMapEntryTask(brokerName, now),
          member);

    }
  }

  private static class ExpireSentMapEntryTask implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerName;
    private final long now;

    public ExpireSentMapEntryTask(String brokerName, long now) {
      this.brokerName = brokerName;
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

      final IMap<DataStructureKey, Long> map = hazelcastInstance.getMap(
          MESSAGE_SENT_MAP_NAME);

      map.entrySet(entry -> {

        // If the entry is null or if the entry expired.
        return entry.getValue() == null || (now - (long) entry.getValue())
            > MESSAGE_SENT_EXPIRATION;

      }).forEach(entry -> {

        // Remove the entry only if it hasn't been modified since
        // we detected it has expired.
        map.remove(entry.getKey(), entry.getValue());
      });
    }
  }

}
