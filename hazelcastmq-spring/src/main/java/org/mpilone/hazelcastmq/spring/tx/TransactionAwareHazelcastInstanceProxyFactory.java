
package org.mpilone.hazelcastmq.spring.tx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.hazelcast.core.HazelcastInstance;

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

  /**
   * Constructs the proxy factory with no target Hazelcast instance. An instance
   * must be set before attempting to create a proxy.
   */
  public TransactionAwareHazelcastInstanceProxyFactory() {
  }

  /**
   * Constructs the proxy factory.
   *
   * @param targetHazelcastInstance the Hazelcast instance to wrap with the
   * proxy
   */
  public TransactionAwareHazelcastInstanceProxyFactory(
      HazelcastInstance targetHazelcastInstance) {
    this.targetHazelcastInstance = targetHazelcastInstance;
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
        new TransactionAwareHazelcastInstanceProxyImpl(targetHazelcastInstance));
  }

  /**
   * The implementation of the proxy that intercepts calls to get transactional
   * objects such as queues, lists, maps, etc. Calls to these operations are
   * delegated to the {@link HazelcastUtils} to potentially participate in an
   * active Spring managed transaction. All other operations are simply passed
   * through to the target instance.
   */
  private static class TransactionAwareHazelcastInstanceProxyImpl implements
      InvocationHandler {

    private final HazelcastInstance targetHazelcastInstance;

    /**
     * Constructs the proxy implementation that will wrap the given instance.
     *
     * @param targetHazelcastInstance the Hazelcast instance that this proxy
     * wraps
     */
    public TransactionAwareHazelcastInstanceProxyImpl(
        HazelcastInstance targetHazelcastInstance) {
      this.targetHazelcastInstance = targetHazelcastInstance;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
        Throwable {

      switch (method.getName()) {
        case "getTargetHazelcastInstance":
          return targetHazelcastInstance;

        case "getQueue":
          return HazelcastUtils.getQueue((String) args[0],
              targetHazelcastInstance);

          // TODO: we only support a transactional queue at this point.
        // Add support for more types in the future.

        case "newTransactionContext":
          // Hazelcast bind the transaction context to the thread
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
  }
}
