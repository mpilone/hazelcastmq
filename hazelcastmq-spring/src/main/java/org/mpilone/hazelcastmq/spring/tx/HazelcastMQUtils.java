package org.mpilone.hazelcastmq.spring.tx;

import java.lang.reflect.*;

import javax.sql.DataSource;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastMQInstanceProxyFactory.TransactionAwareHazelcastMQInstanceProxy;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;


/**
 * <p>
 * Transaction utility methods for a working with a HazelcastMQInstance in a
 * Spring managed transaction. The utility methods will return a transactional
 * context if there is an active Spring managed transaction. If a
 * {@link HazelcastMQTransactionManager} is in use, the bound transaction
 * context is used. If a different transaction manager is in use, a new
 * transaction context may be created and synchronized with the transaction
 * manager as a transactional resource.
 * </p>
 * <p>
 * The {@link HazelcastMQInstance} passed to any method in this class should be
 * an actual instance and not a
 * {@link TransactionAwareHazelcastMQInstanceProxy}. The proxy instance will
 * handle properly binding the resources (via these utility operations).
 * Therefore, either this class should be used directly with a HazelcastMQ
 * instance or the transaction aware proxy should be used, but the two
 * approaches shouldn't be combined.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQUtils {

  public static void releaseHazelcastMQContext(
      HazelcastMQContext mqContext,
      HazelcastMQInstance hazelcastMQInstance) {
    mqContext.close();
  }

  /**
   * <p>
   * Returns a MQ context that will be bound to the current transaction if there
   * is an active HazelcastMQ transaction. If there is no active transaction,
   * null is   * returned. The {@code synchedLocalTransactionAllowed} can be used to allow
   * transaction synchronization with any active transaction, even if it isn't a
   * HazelcastMQ transaction. This is useful for synchronizing a
   * HazelcastMQ transaction to a different PlatformTransactionManager such as a
   * {@link DataSource} transaction.
   * </p>
   *
   * @param hazelcastMQInstance the HazelcastMQ instance to get the context from
   * @param synchedLocalTransactionAllowed true to allow a new HazelcastMQ
   * transaction to be started and synchronized with any existing transaction;
   * false to only return the transactional context if a top-level HazelcastMQ
   * transaction is active
   *
   * @return the transactional context if there is an active HazelcastMQ
   * transaction   * or an active transaction to synchronize to and
   * synchedLocalTransactionAllowed is true; null otherwise
   */
  public static HazelcastMQContext getTransactionalHazelcastMQContext(
      HazelcastMQInstance hazelcastMQInstance,
      boolean synchedLocalTransactionAllowed) {

    HazelcastMQContextHolder conHolder =
        (HazelcastMQContextHolder) TransactionSynchronizationManager.
        getResource(hazelcastMQInstance);

    if (conHolder != null && (conHolder.hasHazelcastMQContext() || conHolder.
        isSynchronizedWithTransaction())) {
      // We are already synchronized with the transaction which means
      // someone already requested a transactional resource or the transaction
      // is being managed at the top level by HazelcastTransactionManager.

      if (!conHolder.hasHazelcastMQContext()) {
        // I think this means we are synchronized with the transaction but
        // we don't have a transactional context because the transaction was
        // suspended. I don't see how we can do this with Hazelcast because
        // it binds the transaction context to the thread and doesn't
        // supported nested transactions. Maybe I'm missing something.

        throw new NestedTransactionNotSupportedException("Trying to resume a "
            + "HazelcastMQ transaction? Can't do that.");
      }
    }
    else if (TransactionSynchronizationManager.isSynchronizationActive()
        && synchedLocalTransactionAllowed) {
      // No holder or no transaction context but we want to be
      // synchronized to the transaction.
      if (conHolder == null) {
        conHolder = new HazelcastMQContextHolder();
        TransactionSynchronizationManager.bindResource(hazelcastMQInstance,
            conHolder);
      }

      HazelcastMQContext mqContext = hazelcastMQInstance.createContext(true);

      conHolder.setHazelcastMQContext(mqContext);
      conHolder.setSynchronizedWithTransaction(true);
      conHolder.setTransactionActive(true);
      TransactionSynchronizationManager.registerSynchronization(
          new HazelcastMQTransactionSynchronization(conHolder,
              hazelcastMQInstance));
    }

    if (conHolder != null && conHolder.getHazelcastMQContext() != null) {

      // Create a proxy on the context so the caller can't directly close
      // the context, commit, or rollback the transaction because it is now
      // being managed by Spring.
      return (HazelcastMQContext) Proxy.newProxyInstance(
          HazelcastMQContext.class.getClassLoader(),
          new Class<?>[]{HazelcastMQContext.class},
          new TransactionAwareHazelcastMQContextProxy(conHolder.
              getHazelcastMQContext()));
    }
    else {
      return null;
    }
  }

  /**
   * A transaction synchronization that can be registered with a non-HazelcastMQ
   * transaction manager to associated a HazelcastMQ transaction to the manager.
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
  private static class HazelcastMQTransactionSynchronization extends ResourceHolderSynchronization<HazelcastMQContextHolder, HazelcastMQInstance> {

    /**
     * The log for this class.
     */
    private static final ILogger log = Logger.getLogger(
        HazelcastMQTransactionSynchronization.class);

    /**
     * Constructs the synchronization.
     *
     * @param resourceHolder the resource holder to associate with the
     * synchronization
     * @param resourceKey the resource key to bind and lookup the resource
     * holder
     */
    public HazelcastMQTransactionSynchronization(
        HazelcastMQContextHolder resourceHolder,
        HazelcastMQInstance resourceKey) {
      super(resourceHolder, resourceKey);
    }

    @Override
    public void afterCommit() {
      log.finest("In HazelcastMQTransactionSynchronization::afterCommit");
      super.afterCommit();
    }

    @Override
    public void afterCompletion(int status) {
      log.finest("In HazelcastMQTransactionSynchronization::afterCompletion");
      super.afterCompletion(status);
    }

    @Override
    public void beforeCommit(boolean readOnly) {
      log.finest("In HazelcastMQTransactionSynchronization::beforeCommit");
      super.beforeCommit(readOnly);
    }

    @Override
    public void beforeCompletion() {
      log.finest("In HazelcastMQTransactionSynchronization::beforeCompletion");
      super.beforeCompletion();
    }

    @Override
    protected void processResourceAfterCommit(
        HazelcastMQContextHolder resourceHolder) {
      log.finest("In HazelcastMQTransactionSynchronization::"
          + "processResourceAfterCommit");

      super.processResourceAfterCommit(resourceHolder);

      resourceHolder.getHazelcastMQContext().commit();
      resourceHolder.clear();
    }

    @Override
    protected boolean shouldReleaseBeforeCompletion() {
      return false;
    }

    @Override
    protected void releaseResource(
        HazelcastMQContextHolder resourceHolder,
        HazelcastMQInstance resourceKey) {
      log.finest("In HazelcastMQTransactionSynchronization::"
          + "releaseResource");

      super.releaseResource(resourceHolder, resourceKey);

      if (resourceHolder.isTransactionActive()) {
        resourceHolder.getHazelcastMQContext().rollback();
      }

      resourceHolder.clear();
    }
  }

  /**
   * A proxy for a {@link HazelcastMQContext} that throws exceptions if the
   * transaction is managed directly via the {@link HazelcastMQContext#commit()}
   * or {@link HazelcastMQContext#rollback() } methods. The transaction is
   * managed by Spring and therefore should never be managed manually.
   */
  private static class TransactionAwareHazelcastMQContextProxy implements
      InvocationHandler {

    private final HazelcastMQContext targetHazelcastMQContext;

    /**
     * Constructs the proxy.
     *
     * @param targetHazelcastMQContext the context to delegate all methods to
     * (other than those that will trigger an exception)
     */
    public TransactionAwareHazelcastMQContextProxy(
        HazelcastMQContext targetHazelcastMQContext) {
      this.targetHazelcastMQContext = targetHazelcastMQContext;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
        Throwable {
      switch (method.getName()) {
        case "commit":
        case "rollback":
          throw new IllegalTransactionStateException(
              "Method %s is not permitted on an MQ context in a Spring managed transaction.");

        case "close":
          // Ignore. We'll close the context at the end of the transaction.
          return null;

        default:
          return method.invoke(targetHazelcastMQContext, args);
      }
    }

    /**
     * Returns the target HazelcastMQContext.
     *
     * @return the target context
     */
    public HazelcastMQContext getTargetHazelcastMQContext() {
      return targetHazelcastMQContext;
    }
  }
}
