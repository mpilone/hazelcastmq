package org.mpilone.hazelcastmq.core;

import java.io.NotSerializableException;
import java.lang.reflect.Proxy;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.mpilone.hazelcastmq.core.HazelcastMQException.TransactionInProgressException;

import com.hazelcast.core.*;
import com.hazelcast.transaction.TransactionContext;

/**
 * Default implementation of the HazelcastMQ context for XA transactions.
 *
 * @author mpilone
 */
class DefaultXaHazelcastMQContext extends DefaultHazelcastMQContext implements
    XAHazelcastMQContext {

  /**
   * The {@link XAResource} to be returned from this context. The resource is
   * actually a proxy to the real resource from the underlying Hazelcast
   * transaction.
   */
  private XAResource xaResource;

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

    refreshTransactionContext();
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
    if (xaResource == null) {
      xaResource = new XAResourceWrapper();
    }

    return xaResource;
  }

  /**
   * Refreshes the transaction context by obtaining a new one from the
   * {@link HazelcastInstance}. Refreshing the context is required because
   * Hazelcast only supports a single transaction per TransactionContext
   * instance so after a commit or rollback a new context must be fetched.
   */
  private void refreshTransactionContext() {
    HazelcastInstance hazelcast =
        DefaultXaHazelcastMQContext.this.config.getHazelcastInstance();
    txnContext = hazelcast.newTransactionContext();
  }

  /**
   * An {@link XAResource} wrapper that properly refreshes a transaction context
   * as needed. All operations are delegated to the underlying Hazelcast
   * {@link TransactionContext#getXaResource()}. Note: Originally this was
   * implemented as a dynamic {@link Proxy} but that caused
   * {@link NotSerializableException}s in Atomikos testing. For some reason
   * implementing the interface directly avoids this problem.
   */
  private class XAResourceWrapper implements XAResource {

    @Override
    public void commit(Xid xid, boolean bln) throws XAException {
      txnContext.getXaResource().commit(xid, bln);
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
      txnContext.getXaResource().end(xid, i);

      refreshTransactionContext();
    }

    @Override
    public void forget(Xid xid) throws XAException {
      txnContext.getXaResource().forget(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
      return txnContext.getXaResource().getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xar) throws XAException {
      return txnContext.getXaResource().isSameRM(xar);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
      return txnContext.getXaResource().prepare(xid);
    }

    @Override
    public Xid[] recover(int i) throws XAException {
      return txnContext.getXaResource().recover(i);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
      txnContext.getXaResource().rollback(xid);
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
      return txnContext.getXaResource().setTransactionTimeout(i);
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
      txnContext.getXaResource().start(xid, i);
    }
  }

}
