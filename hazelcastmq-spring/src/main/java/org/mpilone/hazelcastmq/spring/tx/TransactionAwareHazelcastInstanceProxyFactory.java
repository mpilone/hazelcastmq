
package org.mpilone.hazelcastmq.spring.tx;

import java.lang.reflect.*;

import org.slf4j.*;
import org.springframework.util.Assert;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

/**
 * <p>
 * A factory for creating a Spring transaction aware {@link HazelcastInstance}.
 * The Hazelcast instance created by the factory is a proxy to the target
 * instance and automatically participates in Spring managed transactions.
 * Create a transaction aware proxy is useful in cases where
 * {@link HazelcastUtils} cannot be used directly. The proxy will internally use
 * {@link HazelcastUtils} so application code can (for the most part) ignore
 * Spring transactions while actively participating in them.
 * </p>
 * <p>
 * Refer to the documentation on {@link HazelcastUtils} for some limitations and
 * important warnings related to transactional objects and Spring managed
 * transactions.
 * </p>
 *
 * @author mpilone
 */
public class TransactionAwareHazelcastInstanceProxyFactory {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      TransactionAwareHazelcastInstanceProxyFactory.class);

  private HazelcastInstance targetHazelcastInstance;
  private boolean synchedLocalTransactionAllowed;

  /**
   * Constructs the proxy factory with no target Hazelcast instance. An instance
   * must be set before attempting to create a proxy.
   */
  public TransactionAwareHazelcastInstanceProxyFactory() {
    this(null);
  }

  /**
   * Constructs the proxy factory.
   *
   * @param targetHazelcastInstance the Hazelcast instance to wrap with the
   * proxy
   */
  public TransactionAwareHazelcastInstanceProxyFactory(
      HazelcastInstance targetHazelcastInstance) {
    this(targetHazelcastInstance, false);
  }

  /**
   * Constructs the proxy factory.
   *
   * @param targetHazelcastInstance the Hazelcast instance to wrap with the
   * proxy
   * @param synchedLocalTransactionAllowed true to allow synchronization with
   * non-Hazelcast main transactions; false to only allow transaction
   * participation when the main Spring managed transaction is a Hazelcast
   * transaction
   *
   * @see #setSynchedLocalTransactionAllowed(boolean)
   */
  public TransactionAwareHazelcastInstanceProxyFactory(
      HazelcastInstance targetHazelcastInstance,
      boolean synchedLocalTransactionAllowed) {
    this.targetHazelcastInstance = targetHazelcastInstance;
    this.synchedLocalTransactionAllowed = synchedLocalTransactionAllowed;
  }


  /**
   * <p>
   * Set whether to allow for a local Hazelcast transaction that is synchronized
   * with a Spring-managed transaction (where the main transaction might be a
   * JDBC-based one for a specific DataSource, for example), with the Hazelcast
   * transaction committing right after the main transaction. If not allowed,
   * the given HazelcastInstance needs to handle transaction enlistment
   * underneath the covers (via a
   * {@link HazelcastTransactionManager for example}).
   * </p>
   *
   * <p>
   * Default is "false": If not within a managed transaction that encompasses
   * the underlying HazelcastInstance, standard distributed objects will be
   * returned. Turn this flag on to allow participation in any Spring-managed
   * transaction, with a local Hazelcast transaction synchronized with the main
   * transaction.
   * </p>
   *
   * @param synchedLocalTransactionAllowed true to enable synchronization with
   * any local transaction; false to allow for participation only if the   * main
   * transaction is a Hazelcast managed transaction
   */
  public void setSynchedLocalTransactionAllowed(
      boolean synchedLocalTransactionAllowed) {
    this.synchedLocalTransactionAllowed = synchedLocalTransactionAllowed;
  }

  /**
   * Returns true if a local Hazelcast transaction is allowed to synchronize
   * with the main Spring-managed transaction that is not a Hazelcast
   * transaction.
   *
   * @return true if synchronization is allowed, false if not
   * @see #setSynchedLocalTransactionAllowed(boolean)
   */
  public boolean isSynchedLocalTransactionAllowed() {
    return synchedLocalTransactionAllowed;
  }

  /**
   * Sets the Hazelcast instance to wrap with the proxy.
   *
   * @param targetHazelcastInstance the instance to proxy
   */
  public void setTargetHazelcastInstance(
      HazelcastInstance targetHazelcastInstance) {
    this.targetHazelcastInstance = targetHazelcastInstance;
  }

  /**
   * Returns the Hazelcast instance to wrap with the proxy.
   *
   * @return the instance to proxy
   */
  public HazelcastInstance getTargetHazelcastInstance() {
    return targetHazelcastInstance;
  }

  /**
   * Creates the proxy Hazelcast instance. The returned object implements both
   * the {@link HazelcastInstance} and
   * {@link TransactionAwareHazelcastInstanceProxy} interfaces.
   *
   * @return the proxy for the Hazelcast instance that is aware of Spring
   * managed transactions
   */
  public HazelcastInstance create() {

    Assert.notNull(targetHazelcastInstance,
        "Property 'targetHazelcastInstance' is required.");

    return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.
        getClassLoader(), new Class[]{HazelcastInstance.class,
          TransactionAwareHazelcastInstanceProxy.class},
        new TransactionAwareHazelcastInstanceProxyImpl(targetHazelcastInstance,
            synchedLocalTransactionAllowed));
  }

  /**
   * The implementation of the proxy that intercepts calls to get transactional
   * objects such as queues, lists, maps, etc. Calls to these operations are
   * delegated to the {@link HazelcastUtils} to potentially participate in an
   * active Spring managed transaction. All other operations are simply passed
   * through to the target instance.
   */
  private static class TransactionAwareHazelcastInstanceProxyImpl implements
      InvocationHandler, TransactionAwareHazelcastInstanceProxy {

    private final HazelcastInstance targetHazelcastInstance;
    private final boolean synchedLocalTransactionAllowed;

    /**
     * Constructs the proxy implementation that will wrap the given instance.
     *
     * @param targetHazelcastInstance the Hazelcast instance that this proxy
     * wraps
     */
    public TransactionAwareHazelcastInstanceProxyImpl(
        HazelcastInstance targetHazelcastInstance,
        boolean synchedLocalTransactionAllowed) {
      this.targetHazelcastInstance = targetHazelcastInstance;
      this.synchedLocalTransactionAllowed = synchedLocalTransactionAllowed;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
        Throwable {

      switch (method.getName()) {
        case "getTargetHazelcastInstance":
          return getTargetHazelcastInstance();

        case "isSynchedLocalTransactionAllowed":
          return isSynchedLocalTransactionAllowed();

        case "getQueue":
          IQueue<?> queue = HazelcastUtils.getTransactionalQueue(
              (String) args[0],
              targetHazelcastInstance, synchedLocalTransactionAllowed);

          return queue != null ? queue : method.invoke(targetHazelcastInstance,
              args);

          // TODO: we only support a transactional queue at this point.
        // Add support for more types in the future.

        case "newTransactionContext":
          // Hazelcast binds the transaction context to the thread
          // automatically so if there is a Spring managed transaction this
          // transaction context is going to be the same one and cause issues.
          // It isn't a good idea to mix Spring and manual transaction
          // management until Hazelcast supports non-thread bound transactions.
          log.warn("Calling 'newTransactionContext' on a "
              + "TransactionAwareHazelcastInstanceProxy. The transaction "
              + "context returned may be managed by Spring and therefore "
              + "cause conflicts with the platform transaction manager.");

          return method.invoke(targetHazelcastInstance, args);

        default:
          return method.invoke(targetHazelcastInstance, args);
      }
    }

    @Override
    public HazelcastInstance getTargetHazelcastInstance() {
      return targetHazelcastInstance;
    }

    @Override
    public boolean isSynchedLocalTransactionAllowed() {
      return synchedLocalTransactionAllowed;
    }

  }

  /**
   * An interface that marks a {@link HazelcastInstance} proxy. Implementations
   * of the interface support unwrapping the target instance behind the proxy.
   */
  public interface TransactionAwareHazelcastInstanceProxy {

    /**
     * Returns the target Hazelcast instance behind the proxy.
     *
     * @return the target Hazelcast instance
     */
    HazelcastInstance getTargetHazelcastInstance();

    /**
     * Returns true if a local Hazelcast transaction is allowed to synchronize
     * with the main Spring-managed transaction that is not a Hazelcast
     * transaction.
     *
     * @return true if synchronization is allowed, false if not
     * @see
     * TransactionAwareHazelcastInstanceProxyFactory#setSynchedLocalTransactionAllowed(boolean)
     */
    boolean isSynchedLocalTransactionAllowed();
  }
}
