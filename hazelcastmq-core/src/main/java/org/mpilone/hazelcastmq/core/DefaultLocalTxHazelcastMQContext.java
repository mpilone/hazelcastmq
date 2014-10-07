package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;

/**
 * Default and primary implementation of the HazelcastMQ context.
 *
 * @author mpilone
 */
class DefaultLocalTxHazelcastMQContext extends DefaultHazelcastMQContext
    implements
    HazelcastMQContext {

  /**
   * Constructs the context which may be transacted. The context is a child of
   * the given HazelcastMQ instance.
   *
   * @param transacted true to create a transacted context, false otherwise
   * @param hazelcastMQInstance the parent MQ instance
   */
  public DefaultLocalTxHazelcastMQContext(boolean transacted,
      DefaultHazelcastMQInstance hazelcastMQInstance) {

    super(hazelcastMQInstance);

    if (transacted) {
      HazelcastInstance hazelcast = this.config.getHazelcastInstance();
      txnContext = hazelcast.newTransactionContext();
      txnContext.beginTransaction();
    }
  }

  @Override
  public void commit() {
    if (isTransacted()) {
      contextLock.lock();
      try {
        txnContext.commitTransaction();

        // It appears that Hazelcast doesn't allow a transaction context
        // to be reused so we'll recreate the context after commit.
        txnContext = config.getHazelcastInstance().newTransactionContext();
        txnContext.beginTransaction();
      }
      finally {
        contextLock.unlock();
      }
    }
  }

  @Override
  public void rollback() {
    if (isTransacted()) {

      contextLock.lock();
      try {
        txnContext.rollbackTransaction();

        // It appears that Hazelcast doesn't allow a transaction context
        // to be reused so we'll recreate the context after commit.
        txnContext = config.getHazelcastInstance().newTransactionContext();
        txnContext.beginTransaction();
      }
      finally {
        contextLock.unlock();
      }
    }
  }

}
