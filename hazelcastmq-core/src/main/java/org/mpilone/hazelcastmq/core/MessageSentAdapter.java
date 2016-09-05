package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.*;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
abstract class MessageSentAdapter implements
    EntryAddedListener<DataStructureKey, Long>,
    EntryUpdatedListener<DataStructureKey, Long>,
    EntryMergedListener<DataStructureKey, Long>, Stoppable {

  public final static String MESSAGE_SENT_MAP_NAME =
      "hzmq.messagesent";

  private final StoppableCurrentThreadExecutor executor;

  public MessageSentAdapter() {
    this.executor = new StoppableCurrentThreadExecutor();
  }

  @Override
  public void stop() throws InterruptedException {
    executor.stop();
  }

  @Override
  public boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
    return executor.stop(timeout, unit);
  }

  @Override
  public void entryAdded(EntryEvent<DataStructureKey, Long> event) {
    executor.execute(() -> {
      messageSent(event.getKey());
    });
  }

  @Override
  public void entryMerged(EntryEvent<DataStructureKey, Long> event) {
    executor.execute(() -> {
      messageSent(event.getKey());
    });
  }

  @Override
  public void entryUpdated(EntryEvent<DataStructureKey, Long> event) {
    executor.execute(() -> {
      messageSent(event.getKey());
    });
  }

  abstract void messageSent(DataStructureKey channelKey);

  public static IMap<DataStructureKey, Long> getMapToListen(
      DataStructureContext context) {

    final BaseMap<DataStructureKey, Long> map = context.getMap(
        MESSAGE_SENT_MAP_NAME, false);
    return (IMap<DataStructureKey, Long>) map;
  }

  public static BaseMap<DataStructureKey, Long> getMapToPut(
      DataStructureContext context, boolean joinTransaction) {

    final BaseMap<DataStructureKey, Long> map = context.getMap(
        MESSAGE_SENT_MAP_NAME, joinTransaction);
    return map;
  }

}
