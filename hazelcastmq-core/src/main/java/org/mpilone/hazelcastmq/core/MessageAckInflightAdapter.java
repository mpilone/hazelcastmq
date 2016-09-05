package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.mpilone.hazelcastmq.core.Message;

/**
 *
 * @author mpilone
 */
abstract class MessageAckInflightAdapter implements
    EntryAddedListener<String, MessageAckInflightAdapter.MessageInflight>,
    EntryUpdatedListener<String, MessageAckInflightAdapter.MessageInflight>,
    EntryMergedListener<String, MessageAckInflightAdapter.MessageInflight>,
    ItemListener<MessageAckInflightAdapter.MessageAck>, Stoppable {

  public final static String MESSAGE_INFLIGHT_MAP_NAME = "hzmq.messageinflight";

  public final static String MESSAGE_ACK_QUEUE_NAME = "hzmq.messageack";

  private final StoppableCurrentThreadExecutor executor;

  public MessageAckInflightAdapter() {
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
  public void entryAdded(EntryEvent<String, MessageInflight> evt) {
    executor.execute(() -> {
      messageInflight(evt.getValue());
    });
  }

  @Override
  public void entryUpdated(EntryEvent<String, MessageInflight> evt) {
    executor.execute(() -> {
      messageInflight(evt.getValue());
    });
  }

  @Override
  public void entryMerged(EntryEvent<String, MessageInflight> evt) {
    executor.execute(() -> {
      messageInflight(evt.getValue());
    });
  }

  @Override
  public void itemAdded(ItemEvent<MessageAck> evt) {
    executor.execute(() -> {
      messageAck(evt.getItem());
    });
  }

  @Override
  public void itemRemoved(ItemEvent<MessageAck> evt) {
    executor.execute(() -> {
      messageAck(evt.getItem());
    });
  }

  abstract protected void messageInflight(MessageInflight inflight);

  abstract protected void messageAck(MessageAck ack);

  public static IMap<String, MessageInflight> getMapToListen(
      DataStructureContext context) {

    final BaseMap<String, MessageInflight> map = context.getMap(
        MESSAGE_INFLIGHT_MAP_NAME, false);
    return (IMap<String, MessageInflight>) map;
  }

  public static BaseMap<String, MessageInflight> getMapToPut(
      DataStructureContext context, boolean joinTransaction) {

    final BaseMap<String, MessageInflight> map = context.getMap(
        MESSAGE_INFLIGHT_MAP_NAME, joinTransaction);
    return map;
  }

  public static IQueue<MessageAck> getQueueToListen(
      DataStructureContext context) {

    final BaseQueue<MessageAck> queue = context.getQueue(
        MESSAGE_ACK_QUEUE_NAME, false);
    return (IQueue<MessageAck>) queue;
  }

  public static BaseQueue<MessageAck> getQueueToOffer(
      DataStructureContext context, boolean joinTransaction) {

    final BaseQueue<MessageAck> queue = context.getQueue(
        MESSAGE_ACK_QUEUE_NAME, joinTransaction);
    return queue;
  }

  static class MessageInflight implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Message<?> message;
    private final DataStructureKey channelKey;
    private final long timestamp;

    public MessageInflight(
        Message<?> message, DataStructureKey channelKey, long timestamp) {
      this.message = message;
      this.channelKey = channelKey;
      this.timestamp = timestamp;
    }

    public Message<?> getMessage() {
      return message;
    }

    public DataStructureKey getChannelKey() {
      return channelKey;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  static class MessageAck implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String messageId;
    private final boolean negative;

    public MessageAck(String messageId, boolean negative) {
      this.messageId = messageId;
      this.negative = negative;
    }

    public String getMessageId() {
      return messageId;
    }

    public boolean isNegative() {
      return negative;
    }
  }

}
