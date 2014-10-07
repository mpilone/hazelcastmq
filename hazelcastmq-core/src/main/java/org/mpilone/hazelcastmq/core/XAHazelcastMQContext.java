package org.mpilone.hazelcastmq.core;

import java.io.Closeable;

import javax.transaction.xa.XAResource;

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
 * An XA context can return an {@link XAResource} for participation in a global,
 * two-phase commit transaction. All transaction management must be done through
 * the resource. The transaction must be committed to release the messages to
 * the underlying queue or topic.
 * </p>
 *
 * @author mpilone
 */
public interface XAHazelcastMQContext extends Closeable, HazelcastMQContext {

  /**
   * Throws a {@link HazelcastMQException.TransactionInProgressException}
   * because it should not be called for an XA managed object.
   */
  @Override
  public void commit();

  /**
   * Throws a {@link HazelcastMQException.TransactionInProgressException}
   * because it should not be called for an XA managed object.
   */
  @Override
  public void rollback();

  /**
   * Returns the {@link XAResource} associated with this context. The resource
   * can be registered with a transaction manager to participate in two-phase
   * commits.
   *
   * @return the XA resource for the context
   */
  XAResource getXAResource();

}
