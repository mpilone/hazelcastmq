package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.util.*;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.BaseQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalTaskContext;

/**
 * Default implementation of the channel context. The context can participate in
 * a distributed/managed transaction by setting a managed transactional task
 * context via the {@link #setManagedTransactionContext(com.hazelcast.transaction.TransactionalTaskContext)
 * }.
 *
 * @author mpilone
 */
public class DefaultChannelContext implements ChannelContext {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      DefaultChannelContext.class);

  private final DefaultBroker broker;
  private final HazelcastInstance hazelcastInstance;
  private final Set<DataStructureKey> temporaryDataStructures;
  private final List<Channel> channels;
  private final Object channelMutex;

  private TransactionContext transactionContext;
  private TransactionalTaskContext managedTransactionalTaskContext;
  private boolean autoCommit = true;
  private volatile boolean closed = false;

  public DefaultChannelContext(DefaultBroker broker) {
    this.broker = broker;
    this.hazelcastInstance = broker.getConfig().getHazelcastInstance();
    this.temporaryDataStructures = new HashSet<>();
    this.channels = new LinkedList<>();
    this.channelMutex = new Object();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws HazelcastMQException {
    requireNotClosed();

    if (this.autoCommit == autoCommit) {
      return;
    }

    if (isManagedTx() && autoCommit) {
      throw new HazelcastMQException("Auto-commit cannot be enabled when "
          + "in a distributed/managed transaction.");
    }
    else if (isManagedTx() && !autoCommit) {
      // We will use the managed transaction context so no
      // need to get one from Hz.
    }
    else if (!autoCommit) {

      // Start a transaction to manage.
      transactionContext = hazelcastInstance.newTransactionContext();
      transactionContext.beginTransaction();
    }
    else if (transactionContext != null) {

      // Commit and switch to "auto-commit" which basically means
      // no transaction management.
      transactionContext.commitTransaction();
      transactionContext = null;
    }

    this.autoCommit = autoCommit;
  }

  @Override
  public boolean getAutoCommit() {
    return autoCommit;
  }

  @Override
  public void commit() {
    requireNotClosed();

    if (isManagedTx()) {
      throw new HazelcastMQException("Commit cannot be called when in a "
          + "distributed/managed transaction.");
    }
    else if (autoCommit) {
      throw new HazelcastMQException("Commit cannot be called when in "
          + "auto-commit mode.");
    }

    transactionContext.commitTransaction();
    transactionContext = hazelcastInstance.newTransactionContext();
    transactionContext.beginTransaction();
  }

  @Override
  public void rollback() {
    requireNotClosed();

    if (isManagedTx()) {
      throw new HazelcastMQException("Rollback cannot be called when in a "
          + "distributed/managed transaction.");
    }
    else if (autoCommit) {
      throw new HazelcastMQException("Rollback cannot be called when in "
          + "auto-commit mode.");
    }

    transactionContext.rollbackTransaction();
    transactionContext = hazelcastInstance.newTransactionContext();
    transactionContext.beginTransaction();
  }

  /**
   * Sets the transactional task context to use when part of a
   * distributed/managed transaction. Any active local transaction will be
   * terminated using {@link #setAutoCommit(boolean) setAutoCommit(true)} which
   * does not define if the transaction will be committed or rolled back. The
   * auto-commit mode will then be set to false using the new managed
   * transaction context. Calling this method with a null will cause the context
   * to leave the managed transaction and return to auto-commit mode. Calling
   * this method multiple times with the same context does nothing.
   *
   * @param transactionalTaskContext the new distributed/managed transactional
   * task context or null to leave a managed transaction and return to
   * auto-commit
   */
  public void setManagedTransactionContext(
      TransactionalTaskContext transactionalTaskContext) {
    requireNotClosed();

    // If setting to the same value, do nothing.
    if (transactionalTaskContext == this.managedTransactionalTaskContext) {
      return;
    }

    boolean newManagedTx = transactionalTaskContext != null;

    // Joining a managed transaction.
    if (newManagedTx) {

      // Switch to auto-commit to close any active transaction.
      setAutoCommit(true);

      // Set the managed transaction context so we don't allocate a new
      // one when we switch auto-commit off.
      this.managedTransactionalTaskContext = transactionalTaskContext;

      // Switch to manual commit but now with the managed transactional
      // context.
      setAutoCommit(false);
    }

    // Leaving a managed transaction.
    else {
      this.managedTransactionalTaskContext = null;

      // Switch back to auto-commit.
      setAutoCommit(true);
    }
  }

  /**
   * Checks if the context is closed and throws an exception if it is.
   *
   * @throws HazelcastMQException if the context is closed
   */
  private void requireNotClosed() throws HazelcastMQException {
    if (closed) {
      throw new HazelcastMQException("Context is closed.");
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    // Close any open channels.
    synchronized (channelMutex) {
      new ArrayList<>(channels).stream().forEach(Channel::close);
      channels.clear();
    }

    if (transactionContext != null) {
      transactionContext.rollbackTransaction();
      transactionContext = null;
    }

    // Destroy any temporary data structures.
    temporaryDataStructures.stream().forEach(key -> {
      hazelcastInstance.
          getDistributedObject(key.getServiceName(), key.getName()).destroy();
    });
    temporaryDataStructures.clear();

    getBroker().remove(this);
  }

  void remove(Channel channel) {
    synchronized (channelMutex) {
      channels.remove(channel);
    }
  }

  /**
   * Returns true if this context is in a managed transaction. This is just a
   * simple check to see if a managed transactional task context has been set.
   *
   * @return true if in a distributed/managed transaction
   */
  private boolean isManagedTx() {
    return managedTransactionalTaskContext != null;
  }

  /**
   * Returns the queue with the given name. If <code>transactional</code> is
   * true and there is an active transaction in the context (either local or
   * managed), the queue will be transactional. If false or if there is no
   * transaction, the queue will be non-transactional.
   *
   * @param <E> the type of the elements in the queue
   * @param name the name of the distributed queue
   * @param transactional true to get a transactional queue if possible, false
   * to always get a non-transactional queue
   *
   * @return the queue instance
   */
  <E> BaseQueue<E> getQueue(String name, boolean transactional) {
    BaseQueue<E> queue = null;

    if (transactional) {
      if (managedTransactionalTaskContext != null) {
        queue = managedTransactionalTaskContext.getQueue(name);
      }
      else if (transactionContext != null) {
        queue = transactionContext.getQueue(name);
      }
    }

    if (queue == null) {
      queue = hazelcastInstance.getQueue(name);
    }

    return queue;
  }

  @Override
  public Channel createChannel(DataStructureKey key) {
    requireNotClosed();

    switch (key.getServiceName()) {
      case QueueService.SERVICE_NAME:
        return new QueueChannel(this, key);

      default:
        throw new UnsupportedOperationException(format(
            "Service type [%s] is not currently supported.", key
            .getServiceName()));
    }
  }

  /**
   * Returns the broker that created this context.
   *
   * @return the parent broker
   */
  DefaultBroker getBroker() {
    return broker;
  }

  void addTemporaryDataStructure(DataStructureKey key) {
    temporaryDataStructures.add(key);
  }
}
