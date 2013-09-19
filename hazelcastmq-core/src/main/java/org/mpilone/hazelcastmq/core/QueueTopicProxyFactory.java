package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.TransactionalQueue;

public class QueueTopicProxyFactory {

  private static class TransactionalQueueInvocationHandler<E> implements
      InvocationHandler {

    private TransactionalQueue<E> delegate;

    private final static Map<String, Method> METHOD_MAP = new HashMap<String, Method>();

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

    public TransactionalQueueInvocationHandler(TransactionalQueue<E> queue) {
      this.delegate = queue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[])
     */
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

  private static class BlockingQueueInvocationHandler<E> implements
      InvocationHandler {

    private BlockingQueue<E> delegate;

    private final static Map<String, Method> METHOD_MAP = new HashMap<String, Method>();

    static {
      try {
        METHOD_MAP.put("offer_1",
            BlockingQueue.class.getMethod("offer", Object.class));
        METHOD_MAP.put("offer_3", BlockingQueue.class.getMethod("offer",
            Object.class, long.class, TimeUnit.class));
        METHOD_MAP.put("poll_0", BlockingQueue.class.getMethod("poll"));
        METHOD_MAP.put("poll_2",
            BlockingQueue.class.getMethod("poll", long.class, TimeUnit.class));

      }
      catch (NoSuchMethodException ex) {
        throw new RuntimeException("Could not find method on blocking queue.",
            ex);
      }
    }

    public BlockingQueueInvocationHandler(BlockingQueue<E> queue) {
      this.delegate = queue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[])
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {

      Method delegateMethod = METHOD_MAP.get(method.getName() + "_"
          + args.length);

      if (delegateMethod != null) {
        return delegateMethod.invoke(delegate, args);
      }
      else {
        throw new UnsupportedOperationException(format(
            "Method [%s] is not supported.", method.getName()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static <E> IQueue<E> createQueueProxy(TransactionalQueue<E> queue) {

    InvocationHandler handler = new TransactionalQueueInvocationHandler<E>(
        queue);

    return (IQueue<E>) Proxy.newProxyInstance(
        queue.getClass().getClassLoader(), new Class[] { IQueue.class },
        handler);
  }

  @SuppressWarnings("unchecked")
  public static <E> IQueue<E> createQueueProxy(final BlockingQueue<E> queue) {

    InvocationHandler handler = new BlockingQueueInvocationHandler<E>(queue);

    return (IQueue<E>) Proxy.newProxyInstance(
        queue.getClass().getClassLoader(), new Class[] { IQueue.class },
        handler);
  }

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
        queue.getClass().getClassLoader(), new Class[] { ITopic.class },
        handler);
  }

}
