package org.mpilone.hazelcastmq.spring.tx;
import org.mpilone.hazelcastmq.core.QueueTopicProxyFactory;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastInstanceProxyFactory.TransactionAwareHazelcastInstanceProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.*;

import com.hazelcast.core.*;
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
   * an active transaction. If there is no active transaction, a normal,
   * non-transaction queue is returned.
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
   *
   * @return the queue which may be transactional if there is an active
   * transaction
   */
  public static <E> IQueue<E> getQueue(String name,
      HazelcastInstance hazelcastInstance) {

    HazelcastTransactionContextHolder conHolder =
        getHazelcastTransactionContextHolder(hazelcastInstance);

    if (conHolder != null) {
      TransactionalQueue targetQueue = conHolder.getTransactionContext().
          getQueue(name);

      return QueueTopicProxyFactory.createQueueProxy(targetQueue);
    }
    else {
      // No transaction to synchronize to so we just use a normal,
      // non-transaction data structure.
      return hazelcastInstance.getQueue(name);
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
  private static HazelcastTransactionContextHolder getHazelcastTransactionContextHolder(
      HazelcastInstance hazelcastInstance) {

    HazelcastTransactionContextHolder conHolder =
        (HazelcastTransactionContextHolder) TransactionSynchronizationManager.
        getResource(hazelcastInstance);

    if (conHolder != null && (conHolder.hasTransactionContext() || conHolder.
        isSynchronizedWithTransaction())) {
      // We are already synchronized with the transaction which means
      // someone already requested a transactional resource or the transaction
      // is being managed at the top level by HazelcastTransactionManager.

//      conHolder.requested();
      if (!conHolder.hasTransactionContext()) {
//				logger.debug("Fetching resumed JDBC Connection from DataSource");
//				conHolder.setConnection(dataSource.getConnection());
        throw new UnsupportedOperationException("Trying to resume a Hazelcast "
            + "transaction? Can't do that.");
      }
    }
    else if (TransactionSynchronizationManager.isSynchronizationActive()) {
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

    return conHolder;
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
    private static final Logger log = LoggerFactory.getLogger(
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
      log.debug("In HazelcastTransactionSynchronization::afterCommit");
      super.afterCommit(); 
    }

    @Override
    public void afterCompletion(int status) {
      log.debug("In HazelcastTransactionSynchronization::afterCompletion");
      super.afterCompletion(status);
    }

    @Override
    public void beforeCommit(boolean readOnly) {
      log.debug("In HazelcastTransactionSynchronization::beforeCommit");
      super.beforeCommit(readOnly);
    }

    @Override
    public void beforeCompletion() {
      log.debug("In HazelcastTransactionSynchronization::beforeCompletion");
      super.beforeCompletion();
    }

    @Override
    protected void processResourceAfterCommit(
        HazelcastTransactionContextHolder resourceHolder) {
      log.debug("In HazelcastTransactionSynchronization::"
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
      log.debug("In HazelcastTransactionSynchronization::"
          + "releaseResource");

      super.releaseResource(resourceHolder, resourceKey);

      if (resourceHolder.isTransactionActive()) {
        resourceHolder.getTransactionContext().rollbackTransaction();
      }

      resourceHolder.clear();
    }
  }
}
