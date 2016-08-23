
package org.mpilone.hazelcastmq.core;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * @author mpilone
 */
class MessageSentMapEntryExpirer extends MessageSentMapAdapter {

  /**
   * The amount of time before an entry in the message sent map is considered
   * expired and will be removed. Normally there is one entry per channel so the
   * map reaches a steady state but it is possible to have a lot of entries for
   * temporary channels that can be removed periodically to release resources.
   */
  private static final long MESSAGE_SENT_EXPIRATION = TimeUnit.MINUTES.toMillis(
      2);

  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;

  private Clock clock = Clock.systemUTC();
  private long lastRunTime = 0;

  public MessageSentMapEntryExpirer(BrokerConfig config) {
    this.config = config;
    this.hazelcastInstance = config.getHazelcastInstance();
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    final long now = clock.millis();
    if (now - lastRunTime > MESSAGE_SENT_EXPIRATION) {
      lastRunTime = now;

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
