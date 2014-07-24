package org.mpilone.hazelcastmq.spring.tx;

import org.mpilone.hazelcastmq.core.HazelcastMQContext;
import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * A {@link ResourceHolder} for the {@link HazelcastMQContext} to associate the
 * context with a Spring transaction manager.
 *
 * @author mpilone
 */
class HazelcastMQContextHolder extends ResourceHolderSupport {

  private HazelcastMQContext hazelcastMQContext;
  private boolean transactionActive;

  /**
   * Constructs the context holder with no associated transaction context.
   */
  public HazelcastMQContextHolder() {
  }

  /**
   * Constructs the context holder.
   *
   * @param mqContext the transaction context to hold
   */
  HazelcastMQContextHolder(HazelcastMQContext mqContext) {
    this.hazelcastMQContext = mqContext;
  }

  /**
   * Returns true if the holder is holding an MQ context.
   *
   * @return true if holding a context
   */
  public boolean hasHazelcastMQContext() {
    return this.hazelcastMQContext != null;
  }

  /**
   * Sets the MQ context to be held in the resource holder.
   *
   * @param mqContext the MQ context
   */
  public void setHazelcastMQContext(HazelcastMQContext mqContext) {
    this.hazelcastMQContext = mqContext;
  }

  /**
   * Returns the MQ context in the holder or null if none has been set.
   *
   * @return the MQ context or null
   */
  public HazelcastMQContext getHazelcastMQContext() {
    return this.hazelcastMQContext;
  }

  /**
   * Return whether this holder represents an active, HazelcastMQ-managed
   * transaction.
   *
   * @return true if a transaction is active
   */
  protected boolean isTransactionActive() {
    return this.transactionActive;
  }

  /**
   * Set whether this holder represents an active, HazelcastMQ-managed
   * transaction.
   *
   * @param transactionActive true if a transaction is active
   *
   * @see HazelcastMQTransactionManager
   */
  protected void setTransactionActive(boolean transactionActive) {
    this.transactionActive = transactionActive;
  }

  @Override
  public void clear() {
    super.clear();
    transactionActive = false;

    // Should we drop the MQ context here? -mpilone
  }

}
