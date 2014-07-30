package org.mpilone.hazelcastmq.spring.tx;
import org.mpilone.hazelcastmq.core.QueueTopicProxyFactory;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastInstanceProxyFactory.TransactionAwareHazelcastInstanceProxy;
import org.springframework.transaction.NestedTransactionNotSupportedException;
import org.springframework.transaction.support.*;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.transaction.TransactionContext;

/**
 * <p>
 * Transaction utility methods for a working with a HazelcastInstance in a
 * Spring managed transaction. The utility methods will return transactional
 * instances of the requested object if there is an active Spring managed
 * transaction. If a {@link HazelcastTransactionManager} is in use, the bound
 * transaction context is used. If a different transaction manager is in use, a
 * new transaction context will be created and synchronized with the transaction
 * manager as a transactional resource.
 * </p>
 * <p>
 * The {@link HazelcastInstance} passed to any method in this class should be an
 * actual instance and not a {@link TransactionAwareHazelcastInstanceProxy}. The
 * proxy instance will handle properly binding the resources (via these utility
 * operations). Therefore, either this class should be used directly with a
 * Hazelcast instance or the transaction aware proxy should be used, but the two
 * approaches shouldn't be combined.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastUtils {

  public static void releaseTransactionContext(
      TransactionContext transactionContext,
      HazelcastInstance hazelcastInstance) {
    // no op?
  }

  /**
   * <p>
   * Returns a queue that will be bound to the current transaction if there is
   * an active Hazelcast transaction. If there is no active transaction, null is
   * returned. The {@code synchedLocalTransactionAllowed} can be used to allow
   * transaction synchronization with any active transaction, even if it isn't a
   * Hazelcast transaction. This is useful for synchronizing a Hazelcast
   * transaction to a different PlatformTransactionManager such as a JDBC
   * transaction.
   * </p>
   * <p>
   * WARNING: Hazelcast defines two different interfaces for a transactional
   * queue and a non-transactional queue. This method maps them both to the same
   * interface via generated proxies when needed; however the transactional
   * queue only implements a subset of the normal queue operations. Calling an
   * operation that is not supported on a transactional queue will result in a
   * {@link UnsupportedOperationException}.
   * </p>
   *
   * @param <E> the type of the items in the queue
   * @param name the name of the queue to get
   * @param hazelcastInstance the Hazelcast instance to get the queue from
   * @param synchedLocalTransactionAllowed true to allow a new Hazelcast
   * transaction to be started and synchronized with any existing transaction;
   * false to only return the transactional object if a top-level Hazelcast
   * transaction is active
   *
   * @return the transactional queue if there is an active Hazelcast transaction
   * or an active transaction to synchronize to and
   * synchedLocalTransactionAllowed is true; null otherwise
   */
  public static <E> IQueue<E> getTransactionalQueue(String name,
      HazelcastInstance hazelcastInstance,
      boolean synchedLocalTransactionAllowed) {

    TransactionContext transactionContext =
        getHazelcastTransactionContext(hazelcastInstance,
            synchedLocalTransactionAllowed);

    if (transactionContext != null) {
      TransactionalQueue targetQueue = transactionContext.getQueue(name);
      return QueueTopicProxyFactory.createQueueProxy(targetQueue);
    }
    else {
      // No transaction to synchronize to.
      return null;
    }
  }

  /**
   * Returns the transaction context holder bound to the current transaction. If
   * one cannot be found and a transaction is active, a new one will be created
   * and bound to the thread. If one cannot be found and a transaction is not
   * active, this method returns null.
   *
   * @param hazelcastInstance the Hazelcast instance used to create begin a
   * transaction if needed
   *
   * @return the transaction context holder if a transaction is active, null
   * otherwise
   */
  private static TransactionContext getHazelcastTransactionContext(
      HazelcastInstance hazelcastInstance,
      boolean synchedLocalTransactionAllowed) {

    HazelcastTransactionContextHolder conHolder =
        (HazelcastTransactionContextHolder) TransactionSynchronizationManager.
        getResource(hazelcastInstance);

    if (conHolder != null && (conHolder.hasTransactionContext() || conHolder.
        isSynchronizedWithTransaction())) {
      // We are already synchronized with the transaction which means
      // someone already requested a transactional resource or the transaction
      // is being managed at the top level by HazelcastTransactionManager.

      if (!conHolder.hasTransactionContext()) {
        // I think this means we are synchronized with the transaction but
        // we don't have a transactional context because the transaction was
        // suspended. I don't see how we can do this with Hazelcast because
        // it binds the transaction context to the thread and doesn't
        // supported nested transactions. Maybe I'm missing something.

        throw new NestedTransactionNotSupportedException("Trying to resume a "
            + "Hazelcast transaction? Can't do that.");
      }
    }
    else if (TransactionSynchronizationManager.isSynchronizationActive()
        && synchedLocalTransactionAllowed) {
      // No holder or no transaction context but we want to be
      // synchronized to the transaction.
      if (conHolder == null) {
        conHolder = new HazelcastTransactionContextHolder();
        TransactionSynchronizationManager.bindResource(hazelcastInstance,
            conHolder);
      }

      TransactionContext transactionContext = hazelcastInstance.
          newTransactionContext();
      transactionContext.beginTransaction();

      conHolder.setTransactionContext(transactionContext);
      conHolder.setSynchronizedWithTransaction(true);
      conHolder.setTransactionActive(true);
      TransactionSynchronizationManager.registerSynchronization(
          new HazelcastTransactionSynchronization(conHolder, hazelcastInstance));
    }

    return conHolder != null ? conHolder.getTransactionContext() : null;
  }

  /**
   * A transaction synchronization that can be registered with a non-Hazelcast
   * transaction manager to associated a Hazelcast transaction to the manager.
   * The synchronization will commit or rollback with the main transaction to
   * provide a best attempt synchronized commit.
   *
   * <p>
   * The order of operations appears to be:
   * <ol>
   * <li>Synchronization::beforeCommit</li>
   * <li>Synchronization::beforeCompletion</li>
   * <li>TransactionManager::doCommit</li>
   * <li>Synchronization::afterCommit</li>
   * <li>Synchronization::afterCompletion</li>
   * </ol>
   * </p>
   */
  private static class HazelcastTransactionSynchronization extends ResourceHolderSynchronization<HazelcastTransactionContextHolder, HazelcastInstance> {

    /**
     * The log for this class.
     */
    private static final ILogger log = Logger.getLogger(
        HazelcastTransactionSynchronization.class);

    /**
     * Constructs the synchronization.
     *
     * @param resourceHolder the resource holder to associate with the
     * synchronization
     * @param resourceKey the resource key to bind and lookup the resource
     * holder
     */
    public HazelcastTransactionSynchronization(
        HazelcastTransactionContextHolder resourceHolder,
        HazelcastInstance resourceKey) {
      super(resourceHolder, resourceKey);
    }

    @Override
    public void afterCommit() {
      log.finest("In HazelcastTransactionSynchronization::afterCommit");
      super.afterCommit(); 
    }

    @Override
    public void afterCompletion(int status) {
      log.finest("In HazelcastTransactionSynchronization::afterCompletion");
      super.afterCompletion(status);
    }

    @Override
    public void beforeCommit(boolean readOnly) {
      log.finest("In HazelcastTransactionSynchronization::beforeCommit");
      super.beforeCommit(readOnly);
    }

    @Override
    public void beforeCompletion() {
      log.finest("In HazelcastTransactionSynchronization::beforeCompletion");
      super.beforeCompletion();
    }

    @Override
    protected void processResourceAfterCommit(
        HazelcastTransactionContextHolder resourceHolder) {
      log.finest("In HazelcastTransactionSynchronization::"
          + "processResourceAfterCommit");

      super.processResourceAfterCommit(resourceHolder);

      resourceHolder.getTransactionContext().commitTransaction();
      resourceHolder.clear();
    }

    @Override
    protected boolean shouldReleaseBeforeCompletion() {
      return false;
    }

    @Override
    protected void releaseResource(
        HazelcastTransactionContextHolder resourceHolder,
        HazelcastInstance resourceKey) {
      log.finest("In HazelcastTransactionSynchronization::"
          + "releaseResource");

      super.releaseResource(resourceHolder, resourceKey);

      if (resourceHolder.isTransactionActive()) {
        resourceHolder.getTransactionContext().rollbackTransaction();
      }

      resourceHolder.clear();
    }
  }
}
