package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.map.listener.*;
import java.io.Serializable;
import org.mpilone.hazelcastmq.core.Message;

/**
 * An adapter that implements the multiple listener interfaces required to
 * monitor inflight messages as well as acks. Events are mapped to the {@link #messageAck(org.mpilone.hazelcastmq.core.MessageAckInflightAdapter.MessageAck)
 * } and {@link #messageInflight(org.mpilone.hazelcastmq.core.MessageAckInflightAdapter.MessageInflight)
 * } operations that can be implemented in subclasses to provide functionality.
 *
 * @author mpilone
 */
class MessageAckInflightAdapter implements
    EntryAddedListener<String, MessageAckInflightAdapter.MessageInflight>,
    EntryUpdatedListener<String, MessageAckInflightAdapter.MessageInflight>,
    EntryMergedListener<String, MessageAckInflightAdapter.MessageInflight>,
    ItemListener<MessageAckInflightAdapter.MessageAck> {

  public final static String MESSAGE_INFLIGHT_MAP_NAME = "hzmq.messageinflight";

  public final static String MESSAGE_ACK_QUEUE_NAME = "hzmq.messageack";

  @Override
  public void entryAdded(EntryEvent<String, MessageInflight> evt) {
      messageInflight(evt.getValue());
  }

  @Override
  public void entryUpdated(EntryEvent<String, MessageInflight> evt) {
      messageInflight(evt.getValue());
  }

  @Override
  public void entryMerged(EntryEvent<String, MessageInflight> evt) {
      messageInflight(evt.getValue());
  }

  @Override
  public void itemAdded(ItemEvent<MessageAck> evt) {
      messageAck(evt.getItem());
  }

  @Override
  public void itemRemoved(ItemEvent<MessageAck> evt) {
      messageAck(evt.getItem());
  }

  /**
   * This method should be implemented in subclasses to perform logic based on a
   * message being placed in inflight status. The default implementation does
   * nothing.
   *
   * @param inflight the inflight message details
   */
  protected void messageInflight(MessageInflight inflight) {
    // no op
  }

  /**
   * This method should be implemented in subclasses to perform logic based on a
   * message being acked/nacked. The default implementation does nothing.
   *
   * @param ack the message ack details
   */
  protected void messageAck(MessageAck ack) {
    // no op
  }

  /**
   * A utility method that returns the inflight message map that can be listened
   * to for inflight message status changes.
   *
   * @param context the context used to locate the map
   * @return the map for listening
   */
  public static IMap<String, MessageInflight> getMapToListen(
      DataStructureContext context) {

    final BaseMap<String, MessageInflight> map = context.getMap(
        MESSAGE_INFLIGHT_MAP_NAME, false);
    return (IMap<String, MessageInflight>) map;
  }

  /**
   * A utility method that returns the inflight message map that can be used to
   * put new inflight messages. Depending on the transaction status, the map
   * returned may be transactional.
   *
   * @param context the context used to locate the map
   * @param joinTransaction flag to indicate if the map should join a
   * transaction if there is one
   * @return the inflight map instance
   */
  public static BaseMap<String, MessageInflight> getMapToPut(
      DataStructureContext context, boolean joinTransaction) {

    final BaseMap<String, MessageInflight> map = context.getMap(
        MESSAGE_INFLIGHT_MAP_NAME, joinTransaction);
    return map;
  }

  /**
   * A utility method that returns the message ack queue that can be listened to
   * for new acks.
   *
   * @param context the context used to locate the queue
   * @return the queue for listening
   */
  public static IQueue<MessageAck> getQueueToListen(
      DataStructureContext context) {

    final BaseQueue<MessageAck> queue = context.getQueue(
        MESSAGE_ACK_QUEUE_NAME, false);
    return (IQueue<MessageAck>) queue;
  }

  /**
   * A utility method that returns the message ack queue that can be used to put
   * new message acks. Depending on the transaction status, the queue returned
   * may be transactional.
   *
   * @param context the context used to locate the queue
   * @param joinTransaction flag to indicate if the queue should join a
   * transaction if there is one
   * @return the message ack queue instance
   */
  public static BaseQueue<MessageAck> getQueueToOffer(
      DataStructureContext context, boolean joinTransaction) {

    final BaseQueue<MessageAck> queue = context.getQueue(
        MESSAGE_ACK_QUEUE_NAME, joinTransaction);
    return queue;
  }

  /**
   * The message inflight details such as the channel it is inflight on and the
   * time the message took flight.
   */
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

  /**
   * The message ack details such as the ID of the message to ack and if the ack
   * is negative (i.e. a nack).
   */
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
