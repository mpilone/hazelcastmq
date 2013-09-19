package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.*;

/**
 * The default and primary implementation of a HazelcastMQ consumer. This
 * consumer uses the converter returned by
 * {@link HazelcastMQConfig#getMessageConverter()} to convert all messages
 * received.
 * 
 * @author mpilone
 */
class DefaultHazelcastMQConsumer implements HazelcastMQConsumer {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The parent context of this consumer.
   */
  private DefaultHazelcastMQContext hazelcastMQContext;

  /**
   * The locally cached context configuration.
   */
  private HazelcastMQConfig config;

  /**
   * The unique ID of this consumer. The ID is generated using a Hazelcast
   * {@link IdGenerator} so the ID will be unique across the entire cluster.
   */
  private String id;

  /**
   * The flag which indicates if the consumer is currently active, that is, the
   * context has been started.
   */
  private boolean active;

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
  private String destination;

  /**
   * The listener that responds to queue events in Hazelcast when actively
   * consuming from a queue.
   */
  private HzQueueListener queueListener;

  /**
   * The lock used for thread safety around all receive and shutdown operations.
   */
  private ReentrantLock receiveLock;

  /**
   * The wait condition used when a receive call is made but the consumer isn't
   * active. The condition will be notified when the consumer is started.
   */
  private Condition receiveCondition;

  /**
   * The flag which indicates if the consumer has been closed.
   */
  private boolean closed;

  /**
   * Constructs the consumer which will read from the given destination and is a
   * child of the given context.
   * 
   * @param destination
   *          the destination that this consumer will read from
   * @param hazelcastMQContext
   *          the parent context of this consumer
   */
  DefaultHazelcastMQConsumer(String destination,
      DefaultHazelcastMQContext hazelcastMQContext) {
    super();

    this.destination = destination;
    this.receiveLock = new ReentrantLock();
    this.receiveCondition = receiveLock.newCondition();
    this.closed = false;
    this.active = false;

    this.hazelcastMQContext = hazelcastMQContext;
    this.config = hazelcastMQContext.getHazelcastMQInstance().getConfig();

    HazelcastInstance hazelcast = this.hazelcastMQContext
        .getHazelcastMQInstance().getConfig().getHazelcastInstance();

    IdGenerator idGenerator = hazelcast.getIdGenerator("hazelcastmqconsumer");
    this.id = "hazelcastmqconsumer-" + String.valueOf(idGenerator.newId());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQConsumer#setMessageListener(org
   * .mpilone.hazelcastmq.core.HazelcastMQMessageHandler)
   */
  @Override
  public void setMessageListener(HazelcastMQMessageListener messageListener) {
    this.messageListener = messageListener;

    if (messageListener != null && active) {
      // Signal that we're dispatch ready so the context will drain the queue if
      // there are pending messages.
      hazelcastMQContext.onConsumerDispatchReady(id);
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
   * it to the current message listener.
   * 
   * @return true if a message was dispatched, false otherwise
   */
  boolean receiveAndDispatch() {
    boolean dispatched = false;

    if (messageListener != null) {
      receiveLock.lock();
      try {
        HazelcastMQMessage msg = receiveNoWait();

        if (msg != null) {
          messageListener.onMessage(msg);
          dispatched = true;
        }
      }
      finally {
        receiveLock.unlock();
      }
    }

    return dispatched;
  }

  /**
   * Starts the consumer which will register a queue or topic listener with
   * Hazelcast and enable message consumption (push or pull). If the consumer is
   * already active, this method does nothing.
   */
  void start() {
    if (active) {
      return;
    }

    receiveLock.lock();
    try {
      // Start listening for events. We currently always listen for events even
      // if we don't have a message listener. If this has a performance impact
      // on Hazelcast we may want to only listen if there is a registered
      // message listener that we need to notify.
      IQueue<byte[]> queue = hazelcastMQContext.resolveQueue(destination);
      if (queue != null) {
        // Get the raw queue outside of any transactional context so we can add
        // an item listener.
        queue = config.getHazelcastInstance().getQueue(queue.getName());
        queueListener = new HzQueueListener(queue);
      }

      // If we are a consumer on a topic, immediately start listening for events
      // so we can buffer them for (a)synchronous consumption.
      ITopic<byte[]> topic = hazelcastMQContext.resolveTopic(destination);
      if (topic != null) {
        topicListener = new HzTopicListener(topic);
      }

      active = true;

      if (messageListener != null) {
        // We have a message listener, so tell the context to drain the dispatch
        // ready queues.
        hazelcastMQContext.onConsumerDispatchReady(id);
      }
      else {
        // Signal that any receive requests can continue.
        receiveCondition.signalAll();
      }
    }
    finally {
      receiveLock.unlock();
    }
  }

  /**
   * Stops message consumption (push or pull), removes any listeners from
   * Hazelcast, and returns once all consuming threads have returned.
   */
  void stop() {
    if (!active) {
      return;
    }

    receiveLock.lock();
    try {
      if (topicListener != null) {
        topicListener.shutdown();
        topicListener = null;
      }

      if (queueListener != null) {
        queueListener.shutdown();
        queueListener = null;
      }

      active = false;
      receiveCondition.signalAll();
    }
    finally {
      receiveLock.unlock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#getMessageListener()
   */
  @Override
  public HazelcastMQMessageListener getMessageListener() {
    return messageListener;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#close()
   */
  @Override
  public void close() {

    receiveLock.lock();
    try {
      hazelcastMQContext.onConsumerClose(id);
      closed = true;
      receiveCondition.signalAll();
    }
    finally {
      receiveLock.unlock();
    }
  }

  /**
   * Attempts to receive a message using the given strategy. The method will
   * continue to attempt to receive until either
   * {@link ReceiveStrategy#isRetryable()} returns false, a message is received,
   * or the consumer is stopped.
   * 
   * @param strategy
   *          the strategy to use for receiving the message and determining
   *          retries
   * @return the message or null if no message was received
   */
  private HazelcastMQMessage doReceive(ReceiveStrategy strategy) {

    HazelcastMQMessage msg = null;

    do {
      receiveLock.lock();
      try {

        IQueue<byte[]> queue = hazelcastMQContext.resolveQueue(destination);

        if (queue == null && topicListener == null) {
          throw new HazelcastMQException(format(
              "Destination cannot be resolved [%s].", destination));
        }
        else if (queue == null) {
          queue = topicListener.getQueue();
        }

        byte[] msgData = strategy.receive(queue);
        if (msgData != null) {
          msg = config.getMessageConverter().toMessage(msgData);
        }

        // Check for message expiration if we have a message with expiration
        // time.
        if (msg != null && msg.getHeaders().get(Headers.EXPIRATION) != null) {
          long expirationTime = Long.parseLong(msg.getHeaders().get(
              Headers.EXPIRATION));

          if (expirationTime != 0
              && expirationTime <= System.currentTimeMillis()) {
            log.info("Dropping message [{}] because it has expired.",
                msg.getId());
            msg = null;
          }
        }
      }
      finally {
        receiveLock.unlock();
      }
    }
    while (msg == null && !closed && strategy.isRetryable());

    return msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#receive()
   */
  @Override
  public HazelcastMQMessage receive() {
    return receive(0, TimeUnit.MILLISECONDS);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#receive(long,
   * java.util.concurrent.TimeUnit)
   */
  @Override
  public HazelcastMQMessage receive(long timeout, TimeUnit unit) {

    if (timeout < 0) {
      throw new IllegalArgumentException("Timeout must be >= 0.");
    }

    if (timeout == 0) {
      // Indefinite wait
      return doReceive(new IndefiniteWaitReceive());
    }
    else {
      // Timed wait
      return doReceive(new TimedWaitReceive(TimeUnit.MILLISECONDS.convert(
          timeout, unit)));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#receiveNoWait()
   */
  @Override
  public HazelcastMQMessage receiveNoWait() {
    return doReceive(new NoWaitReceive());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#receiveBody(long,
   * java.util.concurrent.TimeUnit)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQConsumer#receiveBodyNoWait()
   */
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
   * A Hazelcast {@link ItemListener} that notifies the parent context when a
   * new item arrives that could be pushed to a registered
   * {@link HazelcastMQMessageListener}.
   * 
   * @author mpilone
   */
  private class HzQueueListener implements ItemListener<byte[]> {

    private String registrationId;
    private IQueue<byte[]> queue;

    /**
     * Constructs the listener which will listen on the given queue.
     * 
     * @param queue
     *          the queue to listen to
     */
    public HzQueueListener(IQueue<byte[]> queue) {
      this.queue = queue;
      registrationId = this.queue.addItemListener(this, false);
    }

    public void shutdown() {
      queue.removeItemListener(registrationId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.ItemListener#itemAdded(com.hazelcast.core.ItemEvent)
     */
    @Override
    public void itemAdded(ItemEvent<byte[]> arg0) {
      if (messageListener != null) {
        // Notify the context that this consumer is ready for asynchronous
        // dispatch.
        hazelcastMQContext.onConsumerDispatchReady(id);
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.ItemListener#itemRemoved(com.hazelcast.core.ItemEvent)
     */
    @Override
    public void itemRemoved(ItemEvent<byte[]> arg0) {
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
  private class HzTopicListener implements MessageListener<byte[]> {

    private IQueue<byte[]> queue;

    private ITopic<byte[]> msgTopic;

    private String registrationId;

    /**
     * Constructs the topic listener which will listen on the given topic.
     * 
     * @param topic
     *          the topic to listen to
     */
    public HzTopicListener(ITopic<byte[]> topic) {

      this.queue = QueueTopicProxyFactory
          .createQueueProxy(new ArrayBlockingQueue<byte[]>(config
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
    public IQueue<byte[]> getQueue() {
      return queue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MessageListener#onMessage(com.hazelcast.core.Message)
     */
    @Override
    public void onMessage(Message<byte[]> hzMsg) {
      // We always queue the message even if we have a message listener. We'll
      // immediately pull it out of the queue and dispatch in a separate thread.
      // This is important to prevent slow message handlers from blocking topic
      // distribution in Hazelcast.
      if (!queue.offer(hzMsg.getMessageObject())) {
        log.warn(format("In-memory message buffer full for topic [%s]. "
            + "Messages will be lost. Consider increaing the speed of "
            + "the consumer or the message buffer.", msgTopic.getName()));
        return;
      }

      if (messageListener != null) {
        hazelcastMQContext.onConsumerDispatchReady(id);
      }
    }

    public void shutdown() {
      msgTopic.removeMessageListener(registrationId);
      queue.clear();
    }
  }

  /**
   * A strategy used to receive a message from Hazelcast.
   * 
   * @author mpilone
   */
  private interface ReceiveStrategy {
    /**
     * Attempts to receive a raw message from the given queue and return it.
     * Implementations may use different approaches for receiving such as
     * blocking waits or immediate return. Short blocking waits should be
     * combined with the {@link #isRetryable()} method to create longer waits to
     * allow the receive to be cleanly interrupted.
     * 
     * @param queue
     *          the queue to receive from
     * @return the raw message received or null if no message was received
     */
    public byte[] receive(IQueue<byte[]> queue);

    /**
     * Returns true as long as the strategy is retryable, that is, as long as
     * receive should be called.
     * 
     * @return true if {@link #receive(IQueue)} should be called again, false
     *         otherwise
     */
    public boolean isRetryable();
  }

  /**
   * A strategy that returns the first message available or null if no message
   * is available without blocking.
   * 
   * @author mpilone
   */
  private class NoWaitReceive implements ReceiveStrategy {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #receive(com.hazelcast.core.IQueue)
     */
    @Override
    public byte[] receive(IQueue<byte[]> queue) {
      if (closed || !active) {
        return null;
      }

      return queue.poll();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #isRetryable()
     */
    @Override
    public boolean isRetryable() {
      return false;
    }

  }

  /**
   * A strategy that waits for a given amount of time for a message to arrive if
   * no message is immediately available.
   * 
   * @author mpilone
   */
  private class TimedWaitReceive implements ReceiveStrategy {

    private long timeout;

    public TimedWaitReceive(long timeout) {
      this.timeout = timeout;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #receive(com.hazelcast.core.IQueue)
     */
    @Override
    public byte[] receive(IQueue<byte[]> queue) {
      if (closed) {
        return null;
      }

      long t = Math.min(timeout, 500L);
      timeout -= 500L;

      try {
        if (!active) {
          receiveCondition.await(t, TimeUnit.MILLISECONDS);
        }
        else {
          return queue.poll(t, TimeUnit.MILLISECONDS);
        }
      }
      catch (InterruptedException ex) {
        // Ignore for now
      }

      return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #isRetryable()
     */
    @Override
    public boolean isRetryable() {
      return !Thread.interrupted() && timeout > 0 && !closed;
    }
  }

  /**
   * A strategy that will wait indefinitely for a message, only stopping when a
   * message arrives or the consumer is closed.
   * 
   * @author mpilone
   */
  private class IndefiniteWaitReceive implements ReceiveStrategy {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #receive(com.hazelcast.core.IQueue)
     */
    @Override
    public byte[] receive(IQueue<byte[]> queue) {
      if (closed) {
        return null;
      }

      try {
        if (!active) {
          receiveCondition.await(500L, TimeUnit.MILLISECONDS);
        }
        else {
          return queue.poll(500L, TimeUnit.MILLISECONDS);
        }
      }
      catch (InterruptedException ex) {
        // Ignore for now
      }

      return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.DefaultHazelcastMQConsumer.ReceiveStrategy
     * #isRetryable()
     */
    @Override
    public boolean isRetryable() {
      return !Thread.interrupted() && !closed;
    }
  }
}
