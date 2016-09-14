package org.mpilone.hazelcastmq.core;

import com.hazelcast.transaction.TransactionalTaskContext;

/**
 * A interface that indicates the implementor supports setting an
 * externally managed transaction context.
 *
 * @author mpilone
 */
public interface ManagedTransactionContextAware {

  /**
   * Sets the transactional task context to use when part of a
   * distributed/managed transaction. Calling this method multiple times with
   * the same context does nothing. The impact on an active local transaction is
   * implementation dependent.
   *
   * @param transactionalTaskContext the new distributed/managed transactional
   * task context or null to leave a managed transaction and return to local
   * transactions
   */
  void setManagedTransactionContext(
      TransactionalTaskContext transactionalTaskContext);

}
