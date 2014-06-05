package org.mpilone.hazelcastmq.core;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.*;
import com.hazelcast.transaction.TransactionContext;

/**
 * Default and primary implementation of the HazelcastMQ context.
 * 
 * @author mpilone
 */
class DefaultHazelcastMQContext implements HazelcastMQContext {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
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
   * The Hazelcast transaction context if this context is transactional,
   * otherwise null.
   */
  private TransactionContext txnContext;

  /**
   * The flag which indicates if the context is active, that is, if the context
   * has been started.
   */
  private boolean active;

  /**
   * The flag which indicates if the context will be auto started when the first
   * consumer is created. Defaults to true.
   */
  private boolean autoStart = true;

  /**
   * The HazelcastMQ configuration for this context.
   */
  private final HazelcastMQConfig config;

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
  private MessageListenerDispatcher messageListenerDispatcher;

  /**
   * Constructs the context which may be transacted. The context is a child of
   * the given HazelcastMQ instance.
   * 
   * @param transacted
   *          true to create a transacted context, false otherwise
   * @param hazelcastMQInstance
   *          the parent MQ instance
   */
  public DefaultHazelcastMQContext(boolean transacted,
      DefaultHazelcastMQInstance hazelcastMQInstance) {

    this.hazelcastMQInstance = hazelcastMQInstance;
    this.config = this.hazelcastMQInstance.getConfig();
    this.consumerMap = new HashMap<>();
    this.temporaryQueues = new HashSet<>();
    this.temporaryTopics = new HashSet<>();

    HazelcastInstance hazelcast = this.config.getHazelcastInstance();
    IdGenerator idGenerator = hazelcast.getIdGenerator("hazelcastmqcontext");
    this.id = "hazelcastmqcontext-" + String.valueOf(idGenerator.newId());

    if (transacted) {
      txnContext = hazelcast.newTransactionContext();
      txnContext.beginTransaction();
    }
  }

  /**
   * Called by child consumers when the consumer is ready to push a message to a
   * {@link HazelcastMQMessageListener}. The consumer will be queued and handled
   * in the message dispatch thread.
   * 
   * @param id
   *          the ID of the consumer ready for dispatch
   */
  void onConsumerDispatchReady(String id) {
    if (messageListenerDispatcher != null) {
      messageListenerDispatcher.onConsumerDispatchReady(id);
    }
  }

  /**
   * Called by child consumers when the consumer is closed.
   * 
   * @param id
   *          the ID of the consumer being closed
   */
  void onConsumerClose(String id) {
    DefaultHazelcastMQConsumer consumer = consumerMap.remove(id);

    if (consumer != null) {
      consumer.stop();
    }
  }
  
  @Override
  public HazelcastMQConsumer createConsumer(String destination) {
    DefaultHazelcastMQConsumer consumer = new DefaultHazelcastMQConsumer(
        destination, this);

    consumerMap.put(consumer.getId(), consumer);

    if (autoStart && !active) {
      start();
    }
    else if (active) {
      consumer.start();
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
    stop();

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

  @Override
  public void commit() {
    if (isTransacted()) {
      txnContext.commitTransaction();

      txnContext = config.getHazelcastInstance().newTransactionContext();
      txnContext.beginTransaction();
    }
  }

  @Override
  public void rollback() {
    if (isTransacted()) {

      txnContext.rollbackTransaction();

      txnContext = config.getHazelcastInstance().newTransactionContext();
      txnContext.beginTransaction();
    }
  }

  @Override
  public boolean isTransacted() {
    return txnContext != null;
  }

  @Override
  public void start() {
    if (active) {
      return;
    }

    messageListenerDispatcher = new MessageListenerDispatcher();
    config.getExecutor().execute(messageListenerDispatcher);

    for (DefaultHazelcastMQConsumer consumer : consumerMap.values()) {
      consumer.start();
    }

    active = true;
  }

  @Override
  public void stop() {
    if (!active) {
      return;
    }

    for (DefaultHazelcastMQConsumer consumer : consumerMap.values()) {
      consumer.stop();
    }

    messageListenerDispatcher.shutdown();
    messageListenerDispatcher = null;

    active = false;
  }

  @Override
  public void destroyTemporaryDestination(String destination) {
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
        "hazelcastmqcontext-temporary-destination");

    long tempDestId = idGenerator.newId();
    String destination = Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX
        + tempDestId;

    temporaryQueues.add(destination);

    return destination;
  }

  @Override
  public String createTemporaryTopic() {
    IdGenerator idGenerator = config.getHazelcastInstance().getIdGenerator(
        "hazelcastmqcontext-temporary-destination");

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
  public String getId() {
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
   * @param destination
   *          the destination to be resolved
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
   * @param destination
   *          the destination to be resolved
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
  private class MessageListenerDispatcher implements Runnable {
    /**
     * The latch which blocks until shutdown is complete.
     */
    private final CountDownLatch shutdownLatch;

    /**
     * The flag which indicates if the dispatcher should shutdown.
     */
    private volatile boolean shutdown;

    /**
     * A counting semaphore which adds permits when a consumer is ready for
     * dispatch. All permits are drained when a dispatch loop is started.
     */
    private final Semaphore consumerReadySemaphore;

    /**
     * Constructs the dispatcher.
     */
    public MessageListenerDispatcher() {
      this.consumerReadySemaphore = new Semaphore(0);
      shutdownLatch = new CountDownLatch(1);
      shutdown = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      while (!shutdown) {
        try {
          consumerReadySemaphore.acquire();
          consumerReadySemaphore.drainPermits();

          if (!shutdown) {
            doDispatch();
          }
        }
        catch (InterruptedException ex) {
          // Ignore for now
        }
      }

      shutdownLatch.countDown();
    }

    /**
     * Performs a dispatch operation on each child consumer. The consumer is
     * responsible for determining if dispatch, that is, message push is
     * appropriate.
     */
    private void doDispatch() {
      boolean dispatched = true;

      // Keep dispatching as long as one consumer had a successful dispatch.
      while (dispatched && !shutdown) {
        dispatched = false;

        Set<String> consumerIds = new HashSet<>(consumerMap.keySet());
        for (String id : consumerIds) {
          DefaultHazelcastMQConsumer consumer = consumerMap.get(id);
          if (consumer != null) {
            dispatched = consumer.receiveAndDispatch() || dispatched;
          }
        }
      }
    }

    /**
     * Called when a consumer is ready for dispatch. The consumer will be queued
     * and processed in the dispatch thread.
     * 
     * @param id
     *          the ID of the consumer
     */
    public void onConsumerDispatchReady(String id) {
      consumerReadySemaphore.release();
    }

    /**
     * Shuts the dispatcher down.
     */
    public void shutdown() {
      shutdown = true;

      // Send a sentinel to the ready queue to wake it up if needed.
      consumerReadySemaphore.release();

      try {
        shutdownLatch.await(1, TimeUnit.MINUTES);
      }
      catch (InterruptedException ex) {
        log.warn("Interrupted while shutting down. Shutdown may "
            + "not be complete.", ex);
      }
    }
  }
}