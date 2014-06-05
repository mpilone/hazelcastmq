package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.TransactionalQueue;

/**
 * A factory for constructing {@link IQueue} proxies to various underlying queue
 * implementations. The proxy provides a common interface for various queues
 * that do not implement the interface directly.
 *
 * @author mpilone
 */
public class QueueTopicProxyFactory {

  /**
   * Creates an {@link IQueue} proxy around a {@link TransactionalQueue}. This
   * allows for common handling of queues regardless of if they are
   * transactional or not. Ideally Hazelcast's transactional queue would
   * directly implement IQueue but that isn't the case.
   *
   * @param <E> the type of objects in the queue
   * @param queue the transaction queue to create the proxy around
   *
   * @return the proxy to the transactional queue
   */
  @SuppressWarnings("unchecked")
  public static <E> IQueue<E> createQueueProxy(TransactionalQueue<E> queue) {

    InvocationHandler handler = new TransactionalQueueInvocationHandler<>(
        queue);

    return (IQueue<E>) Proxy.newProxyInstance(
        queue.getClass().getClassLoader(), new Class[]{IQueue.class},
        handler);
  }

  /**
   * Creates an {@link IQueue} proxy around a standard
   * {@link ArrayBlockingQueue}. This allows for common handling of queues
   * regardless of if they are standard Java queues or Hazelcast created queues.
   *
   * @param <E> the type of objects in the queue
   * @param queue the blocking queue to create the proxy around
   *
   * @return the proxy to the blocking queue
   */
  @SuppressWarnings("unchecked")
  public static <E> IQueue<E> createQueueProxy(final ArrayBlockingQueue queue) {

    InvocationHandler handler = new AbstractQueueInvocationHandler<>(queue);

    return (IQueue<E>) Proxy.newProxyInstance(
        IQueue.class.getClassLoader(), new Class[]{IQueue.class},
        handler);
  }

  /**
   * Creates an {@link ITopic} proxy on the combination of a
   * {@link TransactionalQueue} and an actual {@link ITopic} instance. The proxy
   * will offer items to the transactional queue when they are published on the
   * topic. All other topic methods are simply passed through to the underlying
   * topic. By offering items to the queue on publish, a transactional topic can
   * be simulated via the ITopic interface.
   *
   * @param <E> the type of items in the topic
   * @param queue the transactional queue to offer all published objects
   * @param topic the underlying topic to handle all other operations
   *
   * @return the proxy around the queue and topic
   */
  @SuppressWarnings("unchecked")
  public static <E> ITopic<E> createTopicProxy(
      final TransactionalQueue<E> queue, final ITopic<E> topic) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
          throws Throwable {

        if (method.getName().equals("publish")) {
          return queue.offer((E) args[0]);
        }
        else {
          return method.invoke(topic, args);
        }
      }
    };

    return (ITopic<E>) Proxy.newProxyInstance(
        ITopic.class.getClassLoader(), new Class[]{ITopic.class},
        handler);
  }

  /**
   * An invocation handler that maps all {@link IQueue} operations to a
   * {@link TransactionalQueue} instance.
   *
   * @param <E> the type of objects in the queue
   */
  private static class TransactionalQueueInvocationHandler<E> implements
      InvocationHandler {

    private final TransactionalQueue<E> delegate;
    private final static Map<String, Method> METHOD_MAP = new HashMap<>();

    static {
      try {
        METHOD_MAP.put("offer_1",
            TransactionalQueue.class.getMethod("offer", Object.class));
        METHOD_MAP.put("offer_3", TransactionalQueue.class.getMethod("offer",
            Object.class, long.class, TimeUnit.class));
        METHOD_MAP.put("poll_0", TransactionalQueue.class.getMethod("poll"));
        METHOD_MAP.put("poll_2", TransactionalQueue.class.getMethod("poll",
            long.class, TimeUnit.class));
        METHOD_MAP.put("size_0", TransactionalQueue.class.getMethod("size"));
        METHOD_MAP.put("getId_0", TransactionalQueue.class.getMethod("getId"));
        METHOD_MAP.put("destroy_0",
            TransactionalQueue.class.getMethod("destroy"));
        METHOD_MAP.put("getName_0",
            TransactionalQueue.class.getMethod("getName"));

      }
      catch (NoSuchMethodException ex) {
        throw new RuntimeException(
            "Could not find method on transactional queue.", ex);
      }
    }

    /**
     * Constructs the handler which will map all operations to the given queue.
     *
     * @param queue the delegate queue
     */
    public TransactionalQueueInvocationHandler(TransactionalQueue<E> queue) {
      this.delegate = queue;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {

      int argCount = args == null ? 0 : args.length;
      Method delegateMethod = METHOD_MAP.get(method.getName() + "_" + argCount);

      if (delegateMethod != null) {
        return delegateMethod.invoke(delegate, args);
      }
      else {
        throw new UnsupportedOperationException(format(
            "Method [%s] is not supported.", method.getName()));
      }
    }
  }

  /**
   * An invocation handler that maps all {@link IQueue} operations to a
   * {@link ArrayBlockingQueue} instance.
   *
   * @param <E> the type of objects in the queue
   */
  private static class AbstractQueueInvocationHandler<E> implements
      InvocationHandler {

    private final ArrayBlockingQueue<E> delegate;
    private final static Map<String, Method> METHOD_MAP = new HashMap<>();

    static {
      try {
        METHOD_MAP.put("offer_1",
            ArrayBlockingQueue.class.getMethod("offer", Object.class));
        METHOD_MAP.put("offer_3", ArrayBlockingQueue.class.getMethod("offer",
            Object.class, long.class, TimeUnit.class));
        METHOD_MAP.put("poll_0", ArrayBlockingQueue.class.getMethod("poll"));
        METHOD_MAP.put("poll_2",
            ArrayBlockingQueue.class.getMethod("poll", long.class,
                TimeUnit.class));
        METHOD_MAP.put("clear_0", ArrayBlockingQueue.class.getMethod("clear"));

      }
      catch (NoSuchMethodException ex) {
        throw new RuntimeException("Could not find method on AbstractQueue.",
            ex);
      }
    }

    /**
     * Constructs the handler which will map all operations to the given queue.
     *
     * @param queue the delegate queue
     */
    public AbstractQueueInvocationHandler(ArrayBlockingQueue<E> queue) {
      this.delegate = queue;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {

      int argCount = args == null ? 0 : args.length;
      Method delegateMethod = METHOD_MAP.get(method.getName() + "_" + argCount);

      if (delegateMethod != null) {
        return delegateMethod.invoke(delegate, args);
      }
      else {
        throw new UnsupportedOperationException(format(
            "Method [%s] is not supported.", method.getName()));
      }
    }
  }

}
