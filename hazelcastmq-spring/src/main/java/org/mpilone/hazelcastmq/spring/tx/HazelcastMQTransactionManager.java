package org.mpilone.hazelcastmq.spring.tx;

import static java.lang.String.format;

import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastMQInstanceProxyFactory.TransactionAwareHazelcastMQInstanceProxy;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.*;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.*;

import com.hazelcast.logging.*;
import com.hazelcast.transaction.*;

/**
 * <p>
 * A {@link PlatformTransactionManager} implementation for a single
 * {@link HazelcastMQInstance}.
 * </p>
 * <p>
 * Application code is required to retrieve transactional contexts via the
 * {@link HazelcastMQUtils} class. Contexts retrieved using these utility
 * methods * will automatically be enrolled in the current transaction if one is active.
 * </p>
 * <p>
 * If code must work with a {@link HazelcastMQInstance} directly, the
 * {@link TransactionAwareHazelcastMQInstanceProxyFactory} can be used to create
 * a proxy to a HazelcastMQInstance. This is also the preferred approach when
 * the HazelcastMQ transaction must be linked to a different transaction manager
 * such as a {@link DataSource} transaction manager.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQTransactionManager extends AbstractPlatformTransactionManager
    implements InitializingBean {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      HazelcastMQTransactionManager.class);

  private HazelcastMQInstance hazelcastMQInstance;

  /**
   * Constructs the transaction manager with no target Hazelcast instance. An
   * instance must be set before use.
   */
  public HazelcastMQTransactionManager() {
    setNestedTransactionAllowed(false);
  }

  /**
   * Constructs the transaction manager.
   *
   * @param hzMQInstance the HazelcastMQInstance to manage the transactions for
   */
  public HazelcastMQTransactionManager(HazelcastMQInstance hzMQInstance) {
    this();

    setHazelcastMQInstance(hzMQInstance);
    afterPropertiesSet();
  }

  /**
   * Returns the HazelcastMQ instance that this transaction manager is managing
   * the transactions for.
   *
   * @return the HazelcastMQ instance or null if one has not been set
   */
  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * Sets the HazelcastMQ instance that this transaction manager is managing the
   * transactions for.
   *
   * @param hazelcastMQInstance the HazelcastMQ instance
   */
  public void setHazelcastMQInstance(HazelcastMQInstance hazelcastMQInstance) {
    if (hazelcastMQInstance instanceof TransactionAwareHazelcastMQInstanceProxy) {
      // If we got a proxy, we need to perform transactions
      // for its underlying target HazelcastInstance, else data access code won't see
      // properly exposed transactions (i.e. transactions for the target HazelcastInstance).
      this.hazelcastMQInstance =
          ((TransactionAwareHazelcastMQInstanceProxy) hazelcastMQInstance).
          getTargetHazelcastMQInstance();
    }
    else {
      this.hazelcastMQInstance = hazelcastMQInstance;
    }
  }

  @Override
  public void afterPropertiesSet() {
    if (getHazelcastMQInstance() == null) {
      throw new IllegalArgumentException(
          "Property 'hazelcastMQInstance' is required");
    }
  }

  @Override
  protected Object doGetTransaction() throws TransactionException {
    HazelcastMQTransactionObject txObject = new HazelcastMQTransactionObject();
//		txObject.setSavepointAllowed(isNestedTransactionAllowed());
    HazelcastMQContextHolder conHolder =
        (HazelcastMQContextHolder) TransactionSynchronizationManager.
        getResource(this.hazelcastMQInstance);
    txObject.setTransactionContextHolder(conHolder, false);
    return txObject;
  }

  @Override
  protected boolean isExistingTransaction(Object transaction) throws
      TransactionException {
    HazelcastMQTransactionObject txObject =
        (HazelcastMQTransactionObject) transaction;
    return (txObject.getHazelcastMQContextHolder() != null && txObject.
        getHazelcastMQContextHolder().isTransactionActive());
  }

  @Override
  protected void doBegin(Object transaction, TransactionDefinition definition)
      throws TransactionException {

    if (definition.getIsolationLevel()
        != TransactionDefinition.ISOLATION_DEFAULT) {
      throw new InvalidIsolationLevelException(
          "HazelcastMQ does not support an isolation level concept");
    }

    HazelcastMQTransactionObject txObject =
        (HazelcastMQTransactionObject) transaction;
    HazelcastMQContext con = null;

    try {
      if (txObject.getHazelcastMQContextHolder() == null || txObject.
          getHazelcastMQContextHolder().isSynchronizedWithTransaction()) {

        TransactionOptions txOps = new TransactionOptions();
        txOps.setTransactionType(TransactionOptions.TransactionType.LOCAL);
        if (definition.getTimeout() > 0) {
          txOps.setTimeout(definition.getTimeout(), TimeUnit.SECONDS);
        }

        HazelcastMQContext newCon = getHazelcastMQInstance().createContext(true);

        if (log.isFinestEnabled()) {
          log.finest(format("Acquired HazelcastMQContext [%s]"
              + " for HazelcastMQ transaction.", newCon));
        }
        txObject.setTransactionContextHolder(
            new HazelcastMQContextHolder(newCon), true);
      }

      txObject.getHazelcastMQContextHolder().
          setSynchronizedWithTransaction(true);
      txObject.getHazelcastMQContextHolder().setTransactionActive(true);
      con = txObject.getHazelcastMQContextHolder().getHazelcastMQContext();

      // Bind the session holder to the thread.
      if (txObject.isNewHazelcastMQContextHolder()) {
        TransactionSynchronizationManager.bindResource(getHazelcastMQInstance(),
            txObject.getHazelcastMQContextHolder());
      }
    }
    catch (Exception ex) {
      HazelcastMQUtils.releaseHazelcastMQContext(con, hazelcastMQInstance);
      throw new CannotCreateTransactionException(
          "Could not create a HazelcastMQ context for transaction", ex);
    }
  }

  @Override
  protected void doCommit(DefaultTransactionStatus status) throws
      TransactionException {
    HazelcastMQTransactionObject txObject =
        (HazelcastMQTransactionObject) status.
        getTransaction();
    HazelcastMQContext con = txObject.getHazelcastMQContextHolder().
        getHazelcastMQContext();

    if (status.isDebug() && log.isFinestEnabled()) {
      log.finest(format("Committing HazelcastMQ transaction on "
          + "HazelcastMQContext [%s].", con));
    }

    try {
      con.commit();
    }
    catch (com.hazelcast.transaction.TransactionException ex) {
      throw new TransactionSystemException(
          "Could not commit HazelcastMQ transaction", ex);
    }
  }

  @Override
  protected void doRollback(DefaultTransactionStatus status) throws
      TransactionException {
    HazelcastMQTransactionObject txObject =
        (HazelcastMQTransactionObject) status.
        getTransaction();
    HazelcastMQContext con = txObject.getHazelcastMQContextHolder().
        getHazelcastMQContext();

    if (status.isDebug() && log.isFinestEnabled()) {
      log.finest(format("Rolling back HazelcastMQ transaction on "
          + "HazelcastMQContext [%s].", con));
    }

    try {
      con.rollback();
    }
    catch (com.hazelcast.transaction.TransactionException ex) {
      throw new TransactionSystemException(
          "Could not rollback HazelcastMQ transaction", ex);
    }
  }

  @Override
  protected void doCleanupAfterCompletion(Object transaction) {
    HazelcastMQTransactionObject txObject =
        (HazelcastMQTransactionObject) transaction;

    // Remove the connection holder from the thread, if exposed.
    if (txObject.isNewHazelcastMQContextHolder()) {
      HazelcastMQContext con = txObject.getHazelcastMQContextHolder().
          getHazelcastMQContext();

      TransactionSynchronizationManager.unbindResource(this.hazelcastMQInstance);

      if (log.isFinestEnabled()) {
        log.finest(format("Releasing HazelcastMQContext [%s] "
            + "after transaction.", con));
      }

      HazelcastMQUtils.releaseHazelcastMQContext(con, this.hazelcastMQInstance);
    }

    txObject.getHazelcastMQContextHolder().clear();
  }

  /**
   * An object representing a managed HazelcastMQ transaction.
   */
  private static class HazelcastMQTransactionObject {

    private HazelcastMQContextHolder hazelcastMQContextHolder;
    private boolean newHazelcastMQContextHolder;

    /**
     * Sets the resource holder being used to hold HazelcastMQ resources in the
     * transaction.
     *
     * @param hazelcastMQContextHolder the MQ context resource holder
     * @param newHolder true if the holder was created for this transaction,
     * false if it already existed
     */
    private void setTransactionContextHolder(
        HazelcastMQContextHolder hazelcastMQContextHolder,
        boolean newHolder) {
      this.hazelcastMQContextHolder = hazelcastMQContextHolder;
      this.newHazelcastMQContextHolder = newHolder;
    }

    /**
     * Returns the resource holder being used to hold HazelcastMQ resources in
     * the transaction.
     *
     * @return the transaction context resource holder
     */
    private HazelcastMQContextHolder getHazelcastMQContextHolder() {
      return hazelcastMQContextHolder;
    }

    /**
     * Returns true if the context holder was created for the current
     * transaction and false if it existed prior to the transaction.
     *
     * @return true if the holder was created for this transaction, false if it
     * already existed
     */
    private boolean isNewHazelcastMQContextHolder() {
      return newHazelcastMQContextHolder;
    }
  }

}
