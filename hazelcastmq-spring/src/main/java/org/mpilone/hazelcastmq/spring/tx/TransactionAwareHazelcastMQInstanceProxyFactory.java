package org.mpilone.hazelcastmq.spring.tx;

import java.lang.reflect.*;

import org.mpilone.hazelcastmq.core.*;
import org.springframework.util.Assert;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

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
public class TransactionAwareHazelcastMQInstanceProxyFactory {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      TransactionAwareHazelcastMQInstanceProxyFactory.class);

  private HazelcastMQInstance targetHazelcastMQInstance;
  private boolean synchedLocalTransactionAllowed;

  /**
   * Constructs the proxy factory with no target Hazelcast instance. An instance
   * must be set before attempting to create a proxy.
   */
  public TransactionAwareHazelcastMQInstanceProxyFactory() {
    this(null);
  }

  /**
   * Constructs the proxy factory.
   *
   * @param targetHazelcastMQInstance the Hazelcast instance to wrap with the
   * proxy
   */
  public TransactionAwareHazelcastMQInstanceProxyFactory(
      HazelcastMQInstance targetHazelcastMQInstance) {
    this(targetHazelcastMQInstance, false);
  }

  /**
   * Constructs the proxy factory.
   *
   * @param targetHazelcastMQInstance the Hazelcast instance to wrap with the
   * proxy
   * @param synchedLocalTransactionAllowed true to allow synchronization with
   * non-Hazelcast main transactions; false to only allow transaction
   * participation when the main Spring managed transaction is a Hazelcast
   * transaction
   *
   * @see #setSynchedLocalTransactionAllowed(boolean)
   */
  public TransactionAwareHazelcastMQInstanceProxyFactory(
      HazelcastMQInstance targetHazelcastMQInstance,
      boolean synchedLocalTransactionAllowed) {
    this.targetHazelcastMQInstance = targetHazelcastMQInstance;
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
   * any local transaction; false to allow for participation only if the * main
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
   * @param targetHazelcastMQInstance the instance to proxy
   */
  public void setTargetHazelcastMQInstance(
      HazelcastMQInstance targetHazelcastMQInstance) {
    this.targetHazelcastMQInstance = targetHazelcastMQInstance;
  }

  /**
   * Returns the Hazelcast instance to wrap with the proxy.
   *
   * @return the instance to proxy
   */
  public HazelcastMQInstance getTargetHazelcastMQInstance() {
    return targetHazelcastMQInstance;
  }

  /**
   * Creates the proxy Hazelcast instance. The returned object implements both
   * the {@link HazelcastMQInstance} and
   * {@link TransactionAwareHazelcastMQInstanceProxy} interfaces.
   *
   * @return the proxy for the HazelcastMQ instance that is aware of Spring
   * managed transactions
   */
  public HazelcastMQInstance create() {

    Assert.notNull(targetHazelcastMQInstance,
        "Property 'targetHazelcastMQInstance' is required.");

    return (HazelcastMQInstance) Proxy.newProxyInstance(
        HazelcastMQInstance.class.
        getClassLoader(), new Class[]{HazelcastMQInstance.class,
          TransactionAwareHazelcastMQInstanceProxy.class},
        new TransactionAwareHazelcastMQInstanceProxyImpl(
            targetHazelcastMQInstance,
            synchedLocalTransactionAllowed));
  }

  /**
   * The implementation of the proxy that intercepts calls to get
   * HazelcastMQContexts. Calls to these operations are delegated to the
   * {@link HazelcastMQUtils} to potentially participate in an active Spring
   * managed transaction. All other operations are simply passed through to the
   * target instance.
   */
  private static class TransactionAwareHazelcastMQInstanceProxyImpl implements
      InvocationHandler, TransactionAwareHazelcastMQInstanceProxy {

    private final HazelcastMQInstance targetHazelcastMQInstance;
    private final boolean synchedLocalTransactionAllowed;

    /**
     * Constructs the proxy implementation that will wrap the given instance.
     *
     * @param targetHazelcastMQInstance the Hazelcast instance that this proxy
     * wraps
     */
    public TransactionAwareHazelcastMQInstanceProxyImpl(
        HazelcastMQInstance targetHazelcastMQInstance,
        boolean synchedLocalTransactionAllowed) {
      this.targetHazelcastMQInstance = targetHazelcastMQInstance;
      this.synchedLocalTransactionAllowed = synchedLocalTransactionAllowed;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
        Throwable {

      switch (method.getName()) {
        case "getTargetHazelcastInstance":
          return getTargetHazelcastMQInstance();

        case "isSynchedLocalTransactionAllowed":
          return isSynchedLocalTransactionAllowed();

        case "createContext":
          HazelcastMQContext mqContext = HazelcastMQUtils.
              getTransactionalHazelcastMQContext(
                  targetHazelcastMQInstance, synchedLocalTransactionAllowed);

          return mqContext != null ? mqContext : method.invoke(
              targetHazelcastMQInstance,
              args);

        default:
          return method.invoke(targetHazelcastMQInstance, args);
      }
    }

    @Override
    public HazelcastMQInstance getTargetHazelcastMQInstance() {
      return targetHazelcastMQInstance;
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
  public interface TransactionAwareHazelcastMQInstanceProxy {

    /**
     * Returns the target Hazelcast instance behind the proxy.
     *
     * @return the target Hazelcast instance
     */
    HazelcastMQInstance getTargetHazelcastMQInstance();

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
