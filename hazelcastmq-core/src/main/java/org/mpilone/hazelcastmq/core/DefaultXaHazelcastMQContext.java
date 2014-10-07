package org.mpilone.hazelcastmq.core;

import javax.transaction.xa.XAResource;

import org.mpilone.hazelcastmq.core.HazelcastMQException.TransactionInProgressException;

import com.hazelcast.core.*;

/**
 * Default implementation of the HazelcastMQ context for XA transactions.
 *
 * @author mpilone
 */
class DefaultXaHazelcastMQContext extends DefaultHazelcastMQContext implements
    XAHazelcastMQContext {

  /**
   * Constructs the context which may be transacted. The context is a child of
   * the given HazelcastMQ instance.
   *
   * @param transacted true to create a transacted context, false otherwise
   * @param hazelcastMQInstance the parent MQ instance
   */
  public DefaultXaHazelcastMQContext(
      DefaultHazelcastMQInstance hazelcastMQInstance) {

    super(hazelcastMQInstance);

    HazelcastInstance hazelcast = this.config.getHazelcastInstance();
    txnContext = hazelcast.newTransactionContext();
  }

  @Override
  public void commit() {
    throw new TransactionInProgressException(
        "Operation is not permitted when transaction is managed via XA.");
  }

  @Override
  public void rollback() {
    throw new TransactionInProgressException(
        "Operation is not permitted when transaction is managed via XA.");
  }

  @Override
  public XAResource getXAResource() {
    return txnContext.getXaResource();
  }

}
