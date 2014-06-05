package org.mpilone.hazelcastmq.core;

import java.io.Closeable;

/**
 * <p>
 * A single threaded MQ context which can create producers and consumers.
 * Contexts are NOT thread-safe and are designed to be used by a single thread
 * at a time. A context is roughly equivalent to the JMS 1.1 concepts of a
 * Connection and Session and therefore follow the same threading rules as the
 * more restrictive Session.
 * </p>
 * <p>
 * When an application needs to send messages it use the createProducer method
 * to create a producer which provides methods to configure and send messages.
 * </p>
 * <p>
 * When an application needs to receive messages it uses the createConsumer
 * method to create a consumer. A consumer provides methods to receive messages
 * either synchronously or asynchronously.
 * </p>
 * <p>
 * A context must be closed using the {@link #close() } method when it is no
 * longer needed to prevent resource leaks.
 * </p>
 * <p>
 * If a context is transacted, a transaction will always be active for all
 * messages produced. The transaction must be committed to release the messages
 * to the underlying queue or topic. Once a transaction is committed or rolled
 * back, another is immediately started.
 * </p>
 *
 * @author mpilone
 */
public interface HazelcastMQContext extends Closeable {

  /**
   * <p>
   * Closes the context.</p>
   * <p>
   * All consumers and producers created by this context will also be
   * immediately closed.</p>
   * <p>
   * Because a context may allocate resources, clients must close the context
   * when it is no longer needed in order to prevent resource leaks. Relying on
   * garbage collection to eventually reclaim these resources may not be timely
   * enough. </p>
   * <p>
   * When this method is invoked, it should not return until message processing
   * has been shut down in an orderly fashion. This means that all message
   * listeners that may have been running have returned, and that all pending
   * receives have returned. A close terminates all pending message receives on
   * the context's consumers. The receives may return with a message or with
   * null, depending on whether there was a message available at   * the time of the
   * close. If one or more of the context's message listeners is processing a
   * message at the time when close is invoked, all the facilities of the
   * context must remain available to those listeners until they return control
   * to the JMS provider.
   * </p>
   * <p>
   * The active transaction will be rolled back and any pending messages will be
   * dropped.</p>
   * <p>
   * A context uses a single thread to run all message listeners in all
   * consumers. Therefore messages will be delivered serially for all consumers
   * created from a single context. If concurrent message delivery is required,
   * use multiple contexts. A MessageListener must not attempt to close its own
   * JMSContext as this would lead to deadlock.</p>
   *
   */
  @Override
  public void close();

  /**
   * <p>
   * Creates a consumer for the specified destination.</p>
   * <p>
   * A client uses a consumer to receive messages that have been sent to a
   * destination.</p>
   *
   * @param destination the destination to access
   *
   * @return a new consumer instance
   */
   HazelcastMQConsumer createConsumer(String destination);

  /**
   * Creates a new producer which can be used to configure and send message.
   * This is identical to calling {@link #createProducer(java.lang.String) }
   * with a null destination.
   *
   * @return the new producer instance
   */
  HazelcastMQProducer createProducer();

  /**
   * Creates a new producer which can be used to configure and send message. The
   * producer will send all messages to the given destination. If the
   * destination is null, the send methods that require a specific destination
   * must be used on the producer.
   *
   * @param destination the destination for all messages from this producer or
   * null to require per send method destinations
   *
   * @return the new producer instance
   */
  HazelcastMQProducer createProducer(String destination);

  /**
   * Commits all messages produced in this transaction and releases any locks
   * currently held.
   */
   void commit();

  /**
   * Rolls back all messages produced in this transaction and releases any locks
   * currently held.
   */
   void rollback();

  /**
   * Returns true if the context is transacted, that is, if transactions are
   * being used for message production.
   *
   * @return true if transacted
   */
   boolean isTransacted();

  /**
   * Starts the delivery of messages to any consumers created by this context.
   * Multiple calls to start will be ignored.
   */
   void start();

  /**
   * <p>
   * Stops the delivery of messages to any consumers created by this context.
   * Delivery can be restarted using the {@link #start() } method.</p>
   * <p>
   * This call blocks until receives and/or message listeners in progress have
   * completed.</p>
   * <p>
   * Stopping a context has no effect on its ability to send messages. Multiple
   * calls to stop will be ignored.</p>
   * <p>
   * A call to stop must not return until delivery of messages has paused. This
   * means that a client can rely on the fact that none of its message listeners
   * will be called and that all threads of control waiting for receive calls to
   * return will not return with a message until the context is restarted. The
   * receive timers for a stopped context continue to advance, so receives may
   * time out while the context is stopped.
   * </p><p>
   * If message listeners are running when stop is invoked, the stop call must
   * wait until all of them have returned before it may return. While these
   * message listeners are completing, they must have the full services of the
   * context available to them.
   * </p><p>
   * A message listener must not attempt to stop its own context as this would
   * lead to deadlock.
   * </p>
   */
   void stop();

  /**
   * Creates a new temporary queue and returns the name of the new destination.
   * A temporary destination will be automatically destroyed when the context is
   * closed.
   *
   * @return the destination name
   */
   String createTemporaryQueue();

  /**
   * Creates a new temporary topic and returns the name of the new destination.
   * A temporary destination will be automatically destroyed when the context is
   * closed.
   *
   * @return the destination name
   */
   String createTemporaryTopic();

  /**
   * Destroys a temporary destination (either queue or topic). The destination
   * must have been created by this context.
   *
   * @param destination the destination to destroy
   */
   void destroyTemporaryDestination(String destination);

  /**
   * Returns true if this context is set to auto start when the first consumer
   * is created. Set this to false to allow for customization of the consumer
   * before messages are available for delivery (rarely needed). The default for
   * all implementations is true.
   *
   * @return true if the context is configured to auto start when the first
   * consumer is created
   */
   boolean isAutoStart();

  /**
   * Sets the auto start flag to enable or disable automatic context start when
   * the first consumer is created.
   *
   * @param autoStart true to enable auto start (the default) or false otherwise
   */
   void setAutoStart(boolean autoStart);
}
