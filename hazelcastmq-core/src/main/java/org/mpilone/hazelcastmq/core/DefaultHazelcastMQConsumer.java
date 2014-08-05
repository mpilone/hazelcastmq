package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import com.hazelcast.core.*;
import com.hazelcast.logging.*;

/**
 * The default and primary implementation of a HazelcastMQ consumer. This
 * consumer uses the converter returned by
 * {@link HazelcastMQConfig#getMessageConverter()} to convert messages from the
 * raw Hazelcast object representation to a {@link HazelcastMQMessage}.
 *
 * @author mpilone
 */
class DefaultHazelcastMQConsumer implements HazelcastMQConsumer {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      DefaultHazelcastMQConsumer.class);

  /**
   * The parent context of this consumer.
   */
  private final DefaultHazelcastMQContext hazelcastMQContext;

  /**
   * The locally cached context configuration.
   */
  private final HazelcastMQConfig config;

  /**
   * The unique ID of this consumer. The ID is generated using a Hazelcast
   * {@link IdGenerator} so the ID will be unique across the entire cluster.
   */
  private final String id;

  /**
   * The listener that responses to topic events in Hazelcast when actively
   * consuming from a topic.
   */
  private HzTopicListener topicListener;

  /**
   * The message listener to push messages to or null for polling only.
   */
  private HazelcastMQMessageListener messageListener;

  /**
   * The destination that this consumer will be reading messages from.
   */
  private final String destination;

  /**
   * The listener that responds to queue events in Hazelcast when actively
   * consuming from a queue.
   */
  private HzQueueListener queueListener;

  /**
   * The lock used for thread safety around all receive and shutdown operations.
   */
  private final ReentrantLock contextLock;

  /**
   * The flag which indicates if the consumer has been closed.
   */
  private boolean closed;

  /**
   * Constructs the consumer which will read from the given destination and is a
   * child of the given context.
   *
   * @param destination the destination that this consumer will read from
   * @param hazelcastMQContext the parent context of this consumer
   */
  DefaultHazelcastMQConsumer(String destination,
      DefaultHazelcastMQContext hazelcastMQContext) {
    super();

    this.destination = destination;
    this.closed = false;
    this.hazelcastMQContext = hazelcastMQContext;
    this.config = hazelcastMQContext.getHazelcastMQInstance().getConfig();
    this.contextLock = hazelcastMQContext.getContextLock();
    this.id = "hzmqconsumer-" + UUID.randomUUID().toString();

    // Start listening for events. We currently always listen for events even
    // if we don't have a message listener. If this has a performance impact
    // on Hazelcast we may want to only listen if there is a registered
    // message listener that we need to notify.
    IQueue<Object> queue = hazelcastMQContext.resolveQueue(destination);
    if (queue != null) {
      // Get the raw queue outside of any transactional context so we can add
      // an item listener.
      queue = config.getHazelcastInstance().getQueue(queue.getName());
      queueListener = new HzQueueListener(queue);
    }

    // If we are a consumer on a topic, immediately start listening for events
    // so we can buffer them for (a)synchronous consumption.
    ITopic<Object> topic = hazelcastMQContext.resolveTopic(destination);
    if (topic != null) {
      topicListener = new HzTopicListener(topic);
    }
  }

  @Override
  public void setMessageListener(HazelcastMQMessageListener messageListener) {

    contextLock.lock();
    try {
      this.messageListener = messageListener;

      if (messageListener != null) {
        // Signal that we're dispatch ready so the context will drain the queue if
        // there are pending messages.
        hazelcastMQContext.getDispatchReadyCondition().signalAll();
      }
    }
    finally {
      contextLock.unlock();
    }
  }

  /**
   * Returns the unique ID of this consumer.
   *
   * @return the unique ID of this consumer
   */
  String getId() {
    return id;
  }

  /**
   * Attempts to receive a message from the destination and dispatch (i.e. push)
   * it to the current message listener. This method must be called from within
   * the context lock.
   *
   * @return true if a message was dispatched, false otherwise
   */
  boolean receiveAndDispatch() {
    boolean dispatched = false;

    if (messageListener != null) {
      HazelcastMQMessage msg = doReceive(-1);

      if (msg != null) {
        // TODO: better error handling.
        messageListener.onMessage(msg);
        dispatched = true;
      }
    }

    return dispatched;
  }

  @Override
  public HazelcastMQMessageListener getMessageListener() {
    return messageListener;
  }

  @Override
  public void close() {
    contextLock.lock();
    try {
      closed = true;

      if (topicListener != null) {
        topicListener.close();
        topicListener = null;
      }

      if (queueListener != null) {
        queueListener.close();
        queueListener = null;
      }

      // Wake up any thread blocking on a receive call.
      hazelcastMQContext.getDispatchReadyCondition().signalAll();
      hazelcastMQContext.onConsumerClose(id);
    }
    finally {
      contextLock.unlock();
    }
  }

  /**
   * Attempts to receive a message, potentially waiting up to the given timeout
   * before returning. The method returns immediately if the consumer is closed.
   *
   * @param timeout the maximum amount of time to wait in milliseconds. A value
   * less than 0 indicates no wait, 0 indicates indefinite wait, and greater
   * than 0 is the time in milliseconds.
   *
   * @return the message or null if no message was received
   */
  private HazelcastMQMessage doReceive(long timeout) {

    HazelcastMQMessage msg = null;
    boolean expired = false;

    contextLock.lock();
    try {
      while (msg == null && !expired && !closed) {

        IQueue<Object> queue = hazelcastMQContext.resolveQueue(destination);

        if (queue == null && topicListener == null) {
          throw new HazelcastMQException(format(
              "Destination cannot be resolved [%s].", destination));
        }
        else if (queue == null) {
          queue = topicListener.getQueue();
        }

        if (hazelcastMQContext.isStarted()) {
          Object msgData = queue.poll();
          if (msgData != null) {
            msg = config.getMessageConverter().toMessage(msgData);
          }
        }

        if (msg == null) {
          if (timeout == 0) {
            // Indefinite wait.
            hazelcastMQContext.getReceiveReadyCondition().await();
          }
          else if (timeout < 0) {
            // No wait.
            expired = true;
          }
          else {
            // Timed wait.
            long waitStart = System.currentTimeMillis();
            expired = !hazelcastMQContext.getReceiveReadyCondition().
                await(timeout, TimeUnit.MILLISECONDS);

            long waitEnd = System.currentTimeMillis();
            timeout -= (waitEnd - waitStart);
          }
        }
      }
    }
    catch (InterruptedException ex) {
      log.warning("Interrupted while waiting on doReceive await.", ex);
    }
    finally {
      contextLock.unlock();
    }

    return msg;
  }

  @Override
  public HazelcastMQMessage receive() {
    assertMessageListenerNull();

    return doReceive(0);
  }

  @Override
  public HazelcastMQMessage receive(long timeout, TimeUnit unit) {
    assertMessageListenerNull();

    if (timeout < 0) {
      throw new IllegalArgumentException("Timeout must be >= 0.");
    }

    return doReceive(TimeUnit.MILLISECONDS.convert(timeout, unit));
  }

  @Override
  public HazelcastMQMessage receiveNoWait() {
    assertMessageListenerNull();

    return doReceive(-1);
  }

  @Override
  public byte[] receiveBody(long timeout, TimeUnit unit) {
    HazelcastMQMessage msg = receive(timeout, unit);

    if (msg != null) {
      return msg.getBody();
    }
    else {
      return null;
    }
  }

  @Override
  public byte[] receiveBodyNoWait() {
    HazelcastMQMessage msg = receiveNoWait();

    if (msg != null) {
      return msg.getBody();
    }
    else {
      return null;
    }
  }

  /**
   * Asserts that the message listener is null or raises an exception.
   *
   * @throws IllegalStateException if a message listener is set
   */
  private void assertMessageListenerNull() throws IllegalStateException {
    if (messageListener != null) {
      throw new IllegalStateException("Consumer cannot be used "
          + "synchronously when an asynchronous message listener is set.");
    }
  }

  /**
   * A Hazelcast {@link ItemListener} that notifies the parent context when a
   * new item arrives that could be pushed to a registered
   * {@link HazelcastMQMessageListener}.
   *
   * @author mpilone
   */
  private class HzQueueListener implements ItemListener<Object>, AutoCloseable {

    private final String registrationId;
    private final IQueue<Object> queue;

    /**
     * Constructs the listener which will listen on the given queue.
     *
     * @param queue the queue to listen to
     */
    public HzQueueListener(IQueue<Object> queue) {
      this.queue = queue;
      registrationId = this.queue.addItemListener(this, false);
    }

    @Override
    public void close() {
      queue.removeItemListener(registrationId);
    }

    @Override
    public void itemAdded(ItemEvent<Object> arg0) {
      hazelcastMQContext.getDispatchReadyCondition().signalAll();
    }

    @Override
    public void itemRemoved(ItemEvent<Object> arg0) {
      // no op
    }

  }

  /**
   * A Hazelcast {@link MessageListener} that queues topic messages into an
   * internal buffer queue for consumption. The number of topic messages queued
   * is controlled by the {@link HazelcastMQConfig#getTopicMaxMessageCount()}
   * value.
   *
   * @author mpilone
   */
  private class HzTopicListener implements MessageListener<Object>,
      AutoCloseable {

    private final IQueue<Object> queue;
    private final ITopic<Object> msgTopic;
    private final String registrationId;

    /**
     * Constructs the topic listener which will listen on the given topic.
     *
     * @param topic the topic to listen to
     */
    public HzTopicListener(ITopic<Object> topic) {

      this.queue = QueueTopicProxyFactory
          .createQueueProxy(new ArrayBlockingQueue<>(config
                  .getTopicMaxMessageCount()));
      this.msgTopic = topic;

      registrationId = topic.addMessageListener(this);
    }

    /**
     * Returns the internal buffer queue that all topic messages will be placed
     * into.
     *
     * @return the internal buffer queue
     */
    public IQueue<Object> getQueue() {
      return queue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MessageListener#onMessage(com.hazelcast.core.Message)
     */
    @Override
    public void onMessage(Message<Object> hzMsg) {
      // We always queue the message even if we have a message listener. We'll
      // immediately pull it out of the queue and dispatch in a separate thread.
      // This is important to prevent slow message handlers from blocking topic
      // distribution in Hazelcast.
      if (!queue.offer(hzMsg.getMessageObject())) {
        log.warning(format("In-memory message buffer full for topic [%s]. "
            + "Messages will be lost. Consider increaing the speed of "
            + "the consumer or the message buffer.", msgTopic.getName()));
        return;
      }

      hazelcastMQContext.getDispatchReadyCondition().signalAll();
    }

    @Override
    public void close() {
      msgTopic.removeMessageListener(registrationId);
      queue.clear();
    }
  }
}
