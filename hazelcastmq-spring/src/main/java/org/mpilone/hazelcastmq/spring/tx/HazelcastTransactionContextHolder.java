
package org.mpilone.hazelcastmq.spring.tx;

import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.ResourceHolderSupport;

import com.hazelcast.transaction.TransactionContext;

/**
 * A {@link ResourceHolder} for the Hazelcast {@link TransactionContext} to
 * associate the context with a Spring transaction manager.
 *
 * @author mpilone
 */
class HazelcastTransactionContextHolder extends ResourceHolderSupport {

  private TransactionContext transactionContext;
  private boolean transactionActive;

  /**
   * Constructs the context holder with no associated transaction context.
   */
  public HazelcastTransactionContextHolder() {
  }

  /**
   * Constructs the context holder.
   *
   * @param transactionContext the transaction context to hold
   */
  HazelcastTransactionContextHolder(TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
  }

  /**
   * Returns true if the holder is holding a transaction context.
   *
   * @return true if holding a context
   */
  public boolean hasTransactionContext() {
    return this.transactionContext != null;
  }

  /**
   * Sets the transaction context to be held in the resource holder.
   *
   * @param transactionContext the transaction context
   */
  void setTransactionContext(TransactionContext transactionContext) {
    this.transactionContext = transactionContext;
  }

  /**
   * Returns the transaction context in the holder or null if none has been set.
   *
   * @return the transaction context or null
   */
  TransactionContext getTransactionContext() {
    return this.transactionContext;
  }

  /**
   * Return whether this holder represents an active, Hazelcast-managed
   * transaction.
   *
   * @return true if a transaction is active
   */
  protected boolean isTransactionActive() {
    return this.transactionActive;
  }

  /**
   * Set whether this holder represents an active, Hazelcast-managed
   * transaction.
   *
   * @param transactionActive true if a transaction is active
   *
   * @see HazelcastTransactionManager
   */
  protected void setTransactionActive(boolean transactionActive) {
    this.transactionActive = transactionActive;
  }

  @Override
  public void clear() {
    super.clear();
    transactionActive = false;

    // Should we drop the transaction context here? -mpilone
  }

}
