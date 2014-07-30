package org.mpilone.hazelcastmq.spring.tx;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastInstanceProxyFactory.TransactionAwareHazelcastInstanceProxy;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.*;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.*;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.*;
import com.hazelcast.transaction.*;

/**
 * <p>
 * A {@link PlatformTransactionManager} implementation for a single
 * {@link HazelcastInstance}.
 * </p>
 * <p>
 * Application code is required to retrieve transactional objects (queues,
 * lists, maps, etc) via the {@link HazelcastUtils} class. Objects retrieved
 * using these utility methods will automatically be enrolled in the current
 * transaction if one is active.
 * </p>
 * <p>
 * If code must work with a {@link HazelcastInstance} directly, the
 * {@link TransactionAwareHazelcastInstanceProxyFactory} can be used to create a
 * proxy to a HazelcastInstance. This is also the preferred approach when the
 * Hazelcast transaction must be linked to a different transaction manager such
 * as a {@link DataSource} transaction manager.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastTransactionManager extends AbstractPlatformTransactionManager
    implements InitializingBean {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      HazelcastTransactionManager.class);

  private HazelcastInstance hazelcastInstance;

  /**
   * Constructs the transaction manager with no target Hazelcast instance. An
   * instance must be set before use.
   */
  public HazelcastTransactionManager() {
    setNestedTransactionAllowed(false);
  }

  /**
   * Constructs the transaction manager.
   *
   * @param hzInstance the HazelcastInstance to manage the transactions for
   */
  public HazelcastTransactionManager(HazelcastInstance hzInstance) {
    this();

    setHazelcastInstance(hzInstance);
    afterPropertiesSet();
  }

  /**
   * Returns the Hazelcast instance that this transaction manager is managing
   * the transactions for.
   *
   * @return the Hazelcast instance or null if one has not been set
   */
  public HazelcastInstance getHazelcastInstance() {
    return hazelcastInstance;
  }

  /**
   * Sets the Hazelcast instance that this transaction manager is managing the
   * transactions for.
   *
   * @param hazelcastInstance the Hazelcast instance
   */
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    if (hazelcastInstance instanceof TransactionAwareHazelcastInstanceProxy) {
      // If we got a proxy, we need to perform transactions
      // for its underlying target HazelcastInstance, else data access code won't see
      // properly exposed transactions (i.e. transactions for the target HazelcastInstance).
      this.hazelcastInstance =
          ((TransactionAwareHazelcastInstanceProxy) hazelcastInstance).
          getTargetHazelcastInstance();
    }
    else {
      this.hazelcastInstance = hazelcastInstance;
    }
  }

  @Override
  public void afterPropertiesSet() {
    if (getHazelcastInstance() == null) {
      throw new IllegalArgumentException(
          "Property 'hazelcastInstance' is required");
    }
  }

  @Override
  protected Object doGetTransaction() throws TransactionException {
    HazelcastTransactionObject txObject = new HazelcastTransactionObject();
//		txObject.setSavepointAllowed(isNestedTransactionAllowed());
    HazelcastTransactionContextHolder conHolder =
        (HazelcastTransactionContextHolder) TransactionSynchronizationManager.
        getResource(this.hazelcastInstance);
    txObject.setTransactionContextHolder(conHolder, false);
    return txObject;
  }

  @Override
  protected boolean isExistingTransaction(Object transaction) throws
      TransactionException {
    HazelcastTransactionObject txObject =
        (HazelcastTransactionObject) transaction;
    return (txObject.getTransactionContextHolder() != null && txObject.
        getTransactionContextHolder().isTransactionActive());
  }

  @Override
  protected void doBegin(Object transaction, TransactionDefinition definition)
      throws TransactionException {

    if (definition.getIsolationLevel()
        != TransactionDefinition.ISOLATION_DEFAULT) {
      throw new InvalidIsolationLevelException(
          "Hazelcast does not support an isolation level concept");
    }

    HazelcastTransactionObject txObject =
        (HazelcastTransactionObject) transaction;
    TransactionContext con = null;

    try {
      if (txObject.getTransactionContextHolder() == null || txObject.
          getTransactionContextHolder().isSynchronizedWithTransaction()) {

        TransactionOptions txOps = new TransactionOptions();
        txOps.setTransactionType(TransactionOptions.TransactionType.LOCAL);
        if (definition.getTimeout() > 0) {
          txOps.setTimeout(definition.getTimeout(), TimeUnit.SECONDS);
        }

        TransactionContext newCon = getHazelcastInstance().
            newTransactionContext(txOps);
        newCon.beginTransaction();

        if (log.isFinestEnabled()) {
          log.finest(format("Acquired TransactionContext [%s]"
              + " for Hazelcast transaction.", newCon));
        }
        txObject.setTransactionContextHolder(
            new HazelcastTransactionContextHolder(newCon), true);
      }

      txObject.getTransactionContextHolder().
          setSynchronizedWithTransaction(true);
      txObject.getTransactionContextHolder().setTransactionActive(true);
      con = txObject.getTransactionContextHolder().getTransactionContext();

      // Bind the session holder to the thread.
      if (txObject.isNewTransactionContextHolder()) {
        TransactionSynchronizationManager.bindResource(getHazelcastInstance(),
            txObject.getTransactionContextHolder());
      }
    }
    catch (Exception ex) {
      HazelcastUtils.releaseTransactionContext(con, hazelcastInstance);
      throw new CannotCreateTransactionException(
          "Could not create a Hazelcast transaction", ex);
    }
  }

  @Override
  protected void doCommit(DefaultTransactionStatus status) throws
      TransactionException {
    HazelcastTransactionObject txObject = (HazelcastTransactionObject) status.
        getTransaction();
    TransactionContext con = txObject.getTransactionContextHolder().
        getTransactionContext();

    if (status.isDebug() && log.isFinestEnabled()) {
      log.finest(format("Committing Hazelcast transaction on "
          + "TransactionContext [%s].", con));
    }

    try {
      con.commitTransaction();
    }
    catch (com.hazelcast.transaction.TransactionException ex) {
      throw new TransactionSystemException(
          "Could not commit Hazelcast transaction", ex);
    }
  }

  @Override
  protected void doRollback(DefaultTransactionStatus status) throws
      TransactionException {
    HazelcastTransactionObject txObject = (HazelcastTransactionObject) status.
        getTransaction();
    TransactionContext con = txObject.getTransactionContextHolder().
        getTransactionContext();

    if (status.isDebug() && log.isFinestEnabled()) {
      log.finest(format("Rolling back Hazelcast transaction on "
          + "TransactionContext [%s].", con));
    }

    try {
      con.rollbackTransaction();
    }
    catch (com.hazelcast.transaction.TransactionException ex) {
      throw new TransactionSystemException(
          "Could not roll back Hazelcast transaction", ex);
    }
  }

  @Override
  protected void doCleanupAfterCompletion(Object transaction) {
    HazelcastTransactionObject txObject =
        (HazelcastTransactionObject) transaction;

    // Remove the connection holder from the thread, if exposed.
    if (txObject.isNewTransactionContextHolder()) {
      TransactionContext con = txObject.getTransactionContextHolder().
          getTransactionContext();

      TransactionSynchronizationManager.unbindResource(this.hazelcastInstance);

      if (log.isFinestEnabled()) {
        log.finest(format("Releasing Hazelcast Transaction [%s] "
            + "after transaction.", con));
      }

      HazelcastUtils.releaseTransactionContext(con, this.hazelcastInstance);
    }

    txObject.getTransactionContextHolder().clear();
  }

  /**
   * An object representing a managed Hazelcast transaction.
   */
  private static class HazelcastTransactionObject {

    private HazelcastTransactionContextHolder transactionContextHolder;
    private boolean newTransactionContextHolder;

    /**
     * Sets the resource holder being used to hold Hazelcast resources in the
     * transaction.
     *
     * @param transactionContextHolder the transaction context resource holder
     * @param newHolder true if the holder was created for this transaction,
     * false if it already existed
     */
    private void setTransactionContextHolder(
        HazelcastTransactionContextHolder transactionContextHolder,
        boolean newHolder) {
      this.transactionContextHolder = transactionContextHolder;
      this.newTransactionContextHolder = newHolder;
    }

    /**
     * Returns the resource holder being used to hold Hazelcast resources in the
     * transaction.
     *
     * @return the transaction context resource holder
     */
    private HazelcastTransactionContextHolder getTransactionContextHolder() {
      return transactionContextHolder;
    }

    /**
     * Returns true if the context holder was created for the current
     * transaction and false if it existed prior to the transaction.
     *
     * @return true if the holder was created for this transaction, false if it
     * already existed
     */
    private boolean isNewTransactionContextHolder() {
      return newTransactionContextHolder;
    }
  }

}
