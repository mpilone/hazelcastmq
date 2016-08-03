package org.mpilone.hazelcastmq.spring.transaction;

import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;

import javax.sql.DataSource;

import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hazelcast.core.*;
import com.hazelcast.spring.transaction.HazelcastTransactionManager;
import com.hazelcast.spring.transaction.ManagedTransactionalTaskContext;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.TransactionalTaskContext;

/**
 * <p>
 * A {@link TransactionalTaskContext} similar to the
 * {@link ManagedTransactionalTaskContext} that can be used to access
 * transactional Hazelcast resources that are bound to a
 * {@link PlatformTransactionManager}. This context implementation should be
 * used whenever the {@link HazelcastTransactionManager} is <em>not</em> being
 * used. This context will automatically synchronize with the platform
 * transaction manager and perform a best effort commit after the primary commit
 * occurs. This is extremely useful in situations where the primary transaction
 * manager is for another resource such as a JDBC {@link DataSource} and the
 * Hazelcast transaction should be synchronized appropriately.
 * </p>
 * <p>
 * All of the get* methods in this class throw a {@link NoTransactionException}
 * if there is no active transaction to synchronize to. In most cases this means
 * the {@link Transactional} annotation was not in used or a transaction was not
 * started manually in the platform transaction manager.
 * </p>
 *
 * @author mpilone
 * @see HazelcastTransactionManager
 */
public class SynchronizedTransactionalTaskContext implements
    TransactionalTaskContext {

  /**
   * The default precedence of the {@link TransactionSynchronization} used for
   * the Hazelcast transaction context.
   */
  public final static int DEFAULT_PRECEDENCE = HIGHEST_PRECEDENCE / 2;

  private final HazelcastInstance hazelcastInstance;

  /**
   * Constructs the transactional task context that will use the given
   * {@link HazelcastInstance} to create a new context if a transaction is
   * active and can be synchronized with.
   *
   * @param hazelcastInstance the Hazelcast instance to use for transaction
   * creation
   */
  public SynchronizedTransactionalTaskContext(
      HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  public <K, V> TransactionalMap<K, V> getMap(String name) {
    return transactionContext().getMap(name);
  }

  @Override
  public <E> TransactionalQueue<E> getQueue(String name) {
    return transactionContext().getQueue(name);
  }

  @Override
  public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
    return transactionContext().getMultiMap(name);
  }

  @Override
  public <E> TransactionalList<E> getList(String name) {
    return transactionContext().getList(name);
  }

  @Override
  public <E> TransactionalSet<E> getSet(String name) {
    return transactionContext().getSet(name);
  }

  @Override
  public <T extends TransactionalObject> T getTransactionalObject(
      String serviceName, String name) {
    return transactionContext().getTransactionalObject(serviceName, name);
  }

  /**
   * Returns the transactionally bound context. If a context is not bound and
   * there is an active transaction (i.e. transaction synchronization is
   * enabled) then a new context will be created, bound, and returned. If there
   * is no transaction active, a {@link NoTransactionException} will be thrown.
   *
   * @return the Hazelcast transaction context
   * @throws NoTransactionException if there is no active transaction to
   * participate in
   */
  private TransactionContext transactionContext() throws NoTransactionException {

    TransactionContext context =
        (TransactionContext) TransactionSynchronizationManager.getResource(
            this);

    if (context != null) {
      return context;
    }
    else if (!TransactionSynchronizationManager.isSynchronizationActive()) {
      throw new NoTransactionException(
          "No TransactionContext with actual transaction available for current thread");
    }
    else {
      // Start a Hazelcast transaction and add a synchronization to the transaction manager.
      context = hazelcastInstance.newTransactionContext();
      context.beginTransaction();

      TransactionSynchronizationManager.bindResource(this, context);
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionContextSynchronization(this, context));

      return context;
    }
  }

  /**
   * A {@link TransactionSynchronization} for the Hazelcast
   * {@link TransactionContext}. The synchronization will properly bind and
   * unbind the context to the transaction and apply the commit and rollback
   * after the main transaction.
   */
  private static class TransactionContextSynchronization extends TransactionSynchronizationAdapter {

    private final TransactionContext context;
    private final Object resourceKey;

    public TransactionContextSynchronization(Object resourceKey,
        TransactionContext context) {
      this.context = context;
      this.resourceKey = resourceKey;
    }

    @Override
    public int getOrder() {
      return DEFAULT_PRECEDENCE;
    }

    @Override
    public void afterCommit() {
      super.afterCommit();

      context.commitTransaction();
    }

    @Override
    public void afterCompletion(int status) {
      super.afterCompletion(status);

      if (status != STATUS_COMMITTED) {
        context.rollbackTransaction();
      }

      // Cleanup resources.
      TransactionSynchronizationManager.unbindResourceIfPossible(resourceKey);
    }

  }
}
