
package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.Serializable;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


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

  private final Executor executor;

  private Clock clock = Clock.systemUTC();
  private long lastRunTime = 0;

  public MessageSentCleanupHandler(Executor executor) {
    this.executor = executor;
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    final long now = clock.millis();
    if (now - lastRunTime > MESSAGE_SENT_EXPIRATION) {
      lastRunTime = now;

      log.fine("Executing task to expire sent map entries.");

//      System.out.println("Executing task to expire sent map entries.");

      executor.execute(new ExpireSentMapEntryTask(now));
    }
  }

  private static class ExpireSentMapEntryTask implements Runnable, Serializable,
      BrokerAware {

    private static final long serialVersionUID = 1L;

    private final long now;

    private Broker broker;

    public ExpireSentMapEntryTask(long now) {
      this.now = now;
    }

    @Override
    public void setBroker(Broker broker) {
      this.broker = broker;
    }

    @Override
    public void run() {

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
