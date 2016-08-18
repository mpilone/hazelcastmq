
package org.mpilone.hazelcastmq.core;

import java.time.Clock;
import java.time.Instant;

import com.hazelcast.core.*;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

/**
 *
 * @author mpilone
 */
abstract class AbstractMessageSentNotifier implements MessageSentNotifier {

  private final static String CHANNEL_SEND_NOTIFIER_MAP_NAME =
      "hzmq.channelsendnotifier";

  protected abstract BaseMap<DataStructureKey, Long> getNotifyMap(String name);

  protected abstract IMap<DataStructureKey, Long> getListenMap(String name);

  private Clock clock = Clock.systemUTC();

  @Override
  public void fireMessageSent(DataStructureKey key) {
    getNotifyMap(CHANNEL_SEND_NOTIFIER_MAP_NAME).put(key, Instant.now(clock).
        toEpochMilli());
  }

  @Override
  public String addMessageSentListener(MessageSentListener listener) {
    return getListenMap(CHANNEL_SEND_NOTIFIER_MAP_NAME).addEntryListener(
        new ListenerAdapter(listener), false);
  }

  @Override
  public String addMessageSentListener(MessageSentListener listener,
      DataStructureKey key) {
    return getListenMap(CHANNEL_SEND_NOTIFIER_MAP_NAME).addEntryListener(
        new ListenerAdapter(listener), key, false);
  }

  @Override
  public boolean removeMessageSentListener(String id) {
    return getListenMap(CHANNEL_SEND_NOTIFIER_MAP_NAME).removeEntryListener(id);
  }

  private static class ListenerAdapter implements
      EntryAddedListener<DataStructureKey, Long>,
      EntryUpdatedListener<DataStructureKey, Long> {

    private final MessageSentListener listener;

    public ListenerAdapter(MessageSentListener listener) {
      this.listener = listener;
    }

    @Override
    public void entryAdded(EntryEvent<DataStructureKey, Long> event) {
      listener.messageSent(new MessageSentEvent(event.getKey()));
    }

    @Override
    public void entryUpdated(EntryEvent<DataStructureKey, Long> event) {
      listener.messageSent(new MessageSentEvent(event.getKey()));
    }

  }

}
