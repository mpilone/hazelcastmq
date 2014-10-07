package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.util.*;
import java.util.concurrent.locks.*;

import com.hazelcast.core.*;
import com.hazelcast.logging.*;
import com.hazelcast.transaction.TransactionContext;

/**
 * Default and primary implementation of the HazelcastMQ context.
 *
 * @author mpilone
 */
abstract class DefaultHazelcastMQContext implements HazelcastMQContext {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      DefaultHazelcastMQContext.class);

  /**
   * The set of active temporary queues.
   */
  private final Set<String> temporaryQueues;

  /**
   * The set of active temporary topics.
   */
  private final Set<String> temporaryTopics;

  /**
   * The flag which indicates if the context is active, that is, if the context
   * has been started.
   */
  private boolean started;

  /**
   * The flag that indicates if the context will be auto started when the first
   * consumer is created. Defaults to true.
   */
  private boolean autoStart = true;

  /**
   * The flag that indicates if the context has been closed.
   */
  private boolean closed = false;

  /**
   * The HazelcastMQ configuration for this context.
   */
  protected final HazelcastMQConfig config;

  /**
   * The unique ID of this context. The ID is generated using a Hazelcast
   * {@link IdGenerator} so the ID will be unique across the entire cluster.
   */
  private final String id;

  /**
   * The map of consumer IDs to active consumers.
   */
  private final Map<String, DefaultHazelcastMQConsumer> consumerMap;

  /**
   * The parent HazelcastMQ instance that owns this topic.
   */
  private final DefaultHazelcastMQInstance hazelcastMQInstance;

  /**
   * The dispatcher which calls consumers that are ready to push a message to
   * message listeners. This is the primary "push" thread of this context.
   */
  private final MessageListenerDispatcher messageListenerDispatcher;

  /**
   * The main lock in the context which is used to synchronize thread access to
   * the start, stop, and close operations during message dispatch.
   */
  protected final ReentrantLock contextLock;

  /**
   * The condition that signals that a message may be ready for synchronous
   * (i.e. polling) receive calls. This condition is managed by the {@link #getContextLock()
   * }.
   */
  private final Condition receiveReadyCondition;

  /**
   * The condition that signals that a message may be ready for asynchronous
   * (i.e. push) dispatch. This condition is not managed by any lock and can be
   * used independently.
   */
  private final Condition dispatchReadyCondition;

  /**
   * The Hazelcast transaction context if this context is transactional,
   * otherwise null.
   */
  protected TransactionContext txnContext;

  @Override
  public boolean isTransacted() {
    return txnContext != null;
  }

  /**
   * Returns true if the context is started (i.e. running) and should therefore
   * be dispatching messages.
   *
   * @return true if the context is started
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * Constructs the context. The context is a child of
   * the given HazelcastMQ instance.
   *
   * @param transacted true to create a transacted context, false otherwise
   * @param hazelcastMQInstance the parent MQ instance
   */
  public DefaultHazelcastMQContext(DefaultHazelcastMQInstance hazelcastMQInstance) {

    this.hazelcastMQInstance = hazelcastMQInstance;
    this.config = this.hazelcastMQInstance.getConfig();
    this.consumerMap = new HashMap<>();
    this.temporaryQueues = new HashSet<>();
    this.temporaryTopics = new HashSet<>();
    this.id = "hzmqcontext-" + UUID.randomUUID().toString();
    this.contextLock = new ReentrantLock();
    this.receiveReadyCondition = this.contextLock.newCondition();
    this.dispatchReadyCondition = new StatefulCondition();

    messageListenerDispatcher = new MessageListenerDispatcher();
    config.getExecutor().execute(messageListenerDispatcher);
  }

  /**
   * Returns the main lock in the context which is used to synchronize thread
   * access to the start, stop, and close operations during message dispatch.
   */
  ReentrantLock getContextLock() {
    return contextLock;
  }

  /**
   * Returns the condition that signals that a message may be ready for
   * synchronous (i.e. polling) receive calls. This condition is managed by the {@link #getContextLock()
   * } therefore the lock must be obtained before using the condition.
   *
   * @return the receive ready condition
   */
  Condition getReceiveReadyCondition() {
    return receiveReadyCondition;
  }

  /**
   * Returns the condition that signals that a message may be ready for
   * asynchronous (i.e. push) dispatch. This condition is not managed by any
   * lock and can be used outside of a lock by any number of threads.
   *
   * @return the dispatch ready condition
   */
  Condition getDispatchReadyCondition() {
    return dispatchReadyCondition;
  }

  /**
   * Called by child consumers when the consumer is closed.
   *
   * @param id the ID of the consumer being closed
   */
  void onConsumerClose(String id) {
    consumerMap.remove(id);
  }

  @Override
  public HazelcastMQConsumer createConsumer(String destination) {
    DefaultHazelcastMQConsumer consumer = new DefaultHazelcastMQConsumer(
        destination, this);

    consumerMap.put(consumer.getId(), consumer);

    if (autoStart && !started) {
      start();
    }

    return consumer;
  }

  @Override
  public void setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
  }

  @Override
  public boolean isAutoStart() {
    return autoStart;
  }

  @Override
  public HazelcastMQProducer createProducer() {
    return createProducer(null);
  }

  @Override
  public HazelcastMQProducer createProducer(String destination) {
    return new DefaultHazelcastMQProducer(destination, this);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    contextLock.lock();
    try {
      this.closed = true;

      // Stop dispatching.
      stop();

      // Close the message dispatcher.
      messageListenerDispatcher.close();

      // Close all consumers
      List<DefaultHazelcastMQConsumer> consumers = new ArrayList<>(
          consumerMap.values());
      for (HazelcastMQConsumer consumer : consumers) {
        consumer.close();
      }

      // Destroy all temporary queues
      for (String destination : temporaryQueues) {
        destroyTemporaryDestination(destination);
      }

      // Destroy all temporary topics
      for (String destination : temporaryTopics) {
        destroyTemporaryDestination(destination);
      }
    }
    finally {
      contextLock.unlock();
    }
  }

  @Override
  public void start() {
    if (started) {
      return;
    }

    contextLock.lock();
    try {
      started = true;
    }
    finally {
      contextLock.unlock();
    }
  }

  @Override
  public void stop() {
    if (!started) {
      return;
    }

    contextLock.lock();
    try {
      started = false;
    }
    finally {
      contextLock.unlock();
    }
  }

  @Override
  public void destroyTemporaryDestination(String destination
  ) {
    if (temporaryQueues.remove(destination)) {
      IQueue<Object> queue = resolveQueue(destination);
      if (queue != null) {
        queue.destroy();
      }
    }
    else if (temporaryTopics.remove(destination)) {
      ITopic<Object> topic = resolveTopic(destination);
      if (topic != null) {
        topic.destroy();
      }
    }
  }

  @Override
  public String createTemporaryQueue() {
    IdGenerator idGenerator = config.getHazelcastInstance().getIdGenerator(
        "hzmqcontext-temporary-destination");

    long tempDestId = idGenerator.newId();
    String destination = Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX
        + tempDestId;

    temporaryQueues.add(destination);

    return destination;
  }

  @Override
  public String createTemporaryTopic() {
    IdGenerator idGenerator = config.getHazelcastInstance().getIdGenerator(
        "hzmqcontext-temporary-destination");

    long tempDestId = idGenerator.newId();
    String destination = Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX
        + tempDestId;

    temporaryTopics.add(destination);

    return destination;
  }

  /**
   * Returns the unique ID of this context.
   *
   * @return the ID of this context
   */
  String getId() {
    return id;
  }

  /**
   * Returns the parent HazelcastMQ instance that owns this context.
   *
   * @return the parent HazelcastMQ instance
   */
  public DefaultHazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * Resolves the given destination if the destination represents a queue. If
   * the destination is not a queue, null is returned. This method takes into
   * account the transactional status of the context, returning a transactional
   * queue if necessary.
   *
   * @param destination the destination to be resolved
   *
   * @return the resolved queue or null
   */
  public IQueue<Object> resolveQueue(String destination) {
    String queueName = null;

    if (destination.startsWith(Headers.DESTINATION_QUEUE_PREFIX)) {
      queueName = destination.substring(Headers.DESTINATION_QUEUE_PREFIX
          .length());

    }
    else if (destination.startsWith(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX)) {
      queueName = destination
          .substring(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX.length());
    }

    if (queueName == null) {
      return null;
    }

    else if (txnContext != null) {
      TransactionalQueue<Object> txnQueue = txnContext.getQueue(queueName);
      return QueueTopicProxyFactory.createQueueProxy(txnQueue);
    }

    else {
      return config.getHazelcastInstance().getQueue(queueName);
    }
  }

  /**
   * Resolves the given destination if the destination represents a topic. If
   * the destination is not a topic, null is returned. This method takes into
   * account the transactional status of the context, returning a transactional
   * topic if necessary.
   *
   * @param destination the destination to be resolved
   *
   * @return the resolved topic or null
   */
  public ITopic<Object> resolveTopic(String destination) {
    String topicName = null;

    if (destination.startsWith(Headers.DESTINATION_TOPIC_PREFIX)) {
      topicName = destination.substring(Headers.DESTINATION_TOPIC_PREFIX
          .length());

    }
    else if (destination.startsWith(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX)) {
      topicName = destination
          .substring(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX.length());
    }

    if (topicName == null) {
      return null;
    }

    ITopic<Object> topic = config.getHazelcastInstance().getTopic(topicName);
    if (txnContext != null) {
      // Hazelcast as of v3.0 doesn't support transactional topics.
      // Therefore we fake it by writing to a transactional queue and on
      // commit, relaying all the messages in the queue to the correct
      // topic. This has the overhead of an extra serialization round
      // trip, but it is better than no topic transactions.
      TransactionalQueue<Object> txnQueue = txnContext
          .getQueue(DefaultHazelcastMQInstance.TXN_TOPIC_QUEUE_NAME);
      return QueueTopicProxyFactory.createTopicProxy(txnQueue, topic);
    }
    else {
      return topic;
    }
  }

  /**
   * The message dispatcher for all message listeners across all child
   * consumers. As per the JMS specification, there is one message dispatch
   * thread for a context which services all consumers. This helps to keep the
   * threading model relatively simple for implementations of
   * {@link HazelcastMQMessageListener}.
   *
   * @author mpilone
   *
   */
  private class MessageListenerDispatcher implements Runnable, AutoCloseable {

    /**
     * The flag which indicates if the dispatcher should shutdown.
     */
    private volatile boolean closed;

    /**
     * Constructs the dispatcher.
     */
    public MessageListenerDispatcher() {
      closed = false;
    }

    @Override
    public void run() {
      while (!closed) {

        contextLock.lock();
        try {
          doDispatch();
        }
        finally {
          contextLock.unlock();
        }

        try {
          dispatchReadyCondition.await();
        }
        catch (InterruptedException ex) {
          // ignore
        }
      }
    }

    /**
     * Performs a dispatch operation on each child consumer. The consumer is
     * responsible for determining if dispatch, that is, message push is
     * appropriate.
     */
    private void doDispatch() {
      boolean dispatched;

      // Perform all push receives to message listeners. We'll keep
      // dispatching as long as one consumer had a successful push in
      // order to drain all queues and topics.
      do {
        if (log.isFinestEnabled()) {
          log.finest(format("Initiating receive and dispatch on "
              + "[%d] consumers.", consumerMap.size()));
        }

        dispatched = false;
        for (DefaultHazelcastMQConsumer consumer : consumerMap.values()) {
          dispatched = consumer.receiveAndDispatch() || dispatched;
        }
      } while (dispatched);

      // Notify any thread doing a polling receive.
      receiveReadyCondition.signalAll();
    }

    /**
     * Shuts the dispatcher down.
     */
    @Override
    public void close() {
      closed = true;
    }
  }
}
