package org.mpilone.hazelcastmq.spring.transaction;

import com.hazelcast.spring.transaction.ManagedTransactionalTaskContext;
import com.hazelcast.transaction.TransactionalTaskContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.mpilone.hazelcastmq.core.*;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static java.lang.String.format;

/**
 * A {@link Broker} proxy that is aware of a distributed/managed transaction via
 * a {@link ManagedTransactionalTaskContext} or
 * {@link SynchronizedTransactionalTaskContext}. If a managed transaction exists
 * or can be started, the broker will bind a channel context to the transaction
 * (i.e. thread) and return the same instance for all calls to {@link #createChannelContext()
 * } for the duration of the transaction. Therefore the context commit and close
 * operations will be synchronized with the managed transaction. This class
 * should be used any time the channel context should be synchronized with a
 * managed transaction.
 *
 * @author mpilone
 */
public class TransactionAwareBrokerProxy implements Broker {

  private final TransactionalTaskContext transactionalTaskContext;
  private final Broker broker;
  private final static String TEST_TRANSACTIONAL_QUEUE_NAME = "hzmq."
      + TransactionAwareBrokerProxy.class.getSimpleName();

  public TransactionAwareBrokerProxy(
      ManagedTransactionalTaskContext transactionalTaskContext, Broker broker) {
    this((TransactionalTaskContext) transactionalTaskContext, broker);
  }

  public TransactionAwareBrokerProxy(
      SynchronizedTransactionalTaskContext transactionalTaskContext,
      Broker broker) {
    this((TransactionalTaskContext) transactionalTaskContext, broker);
  }

  private TransactionAwareBrokerProxy(
      TransactionalTaskContext transactionalTaskContext,
      Broker broker) {
    this.transactionalTaskContext = transactionalTaskContext;
    this.broker = broker;
  }

  @Override
  public ChannelContext createChannelContext() {

    // See if we have a resource already bound for this transaction.
    ChannelContext channelContext =
        (ChannelContext) TransactionSynchronizationManager.getResource(this);

    if (channelContext != null) {
      return channelContext;
    }
    else {
      // See if there is an active Hazelcast transaction that we should 
      // synchronize with.
      boolean txActive;
      try {
        transactionalTaskContext.getQueue(TEST_TRANSACTIONAL_QUEUE_NAME);
        txActive = true;
      }
      catch (NoTransactionException ex) {
        txActive = false;
      }

      if (txActive) {
        channelContext = broker.createChannelContext();

        // Let the context know about the tx synchronized transactional
        // task context?
        if (channelContext instanceof DefaultChannelContext) {
          ((DefaultChannelContext) channelContext).setManagedTransactionContext(
              transactionalTaskContext);
        }
        else {
          throw new UnsupportedOperationException(format("Joining a "
              + "distributed/managed transaction is not support for a "
              + "channel context of type [%s].", channelContext.getClass().
              getName()));
        }

        // Register with the transaction manager.
        TransactionSynchronizationManager.bindResource(this, channelContext);
        TransactionSynchronizationManager.registerSynchronization(
            new ChannelContextSynchronization(this, channelContext));

        // Wrap the channel context so the caller can't really close it. It
        // will be closed when the transaction ends.
        return (ChannelContext) Proxy.newProxyInstance(ChannelContext.class.
            getClassLoader(),
            new Class<?>[]{ChannelContext.class},
            new ChannelContextInvocationHandler(channelContext));
      }
      else {
        // No active transaction to synchronize with so we just pass the
        // call through like normal.
        return broker.createChannelContext();
      }
    }
  }

  @Override
  public void close() {
    // Should we check for an active synchronization and throw an exception?
    // Seems like a real edge case.

    broker.close();
  }

  @Override
  public BrokerConfig getConfig() {
    return broker.getConfig();
  }

  @Override
  public RouterContext createRouterContext() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private static class ChannelContextSynchronization extends TransactionSynchronizationAdapter {

    private final ChannelContext channelContext;
    private final Object resourceKey;

    public ChannelContextSynchronization(Object resourceKey,
        ChannelContext channelContext) {
      this.channelContext = channelContext;
      this.resourceKey = resourceKey;
    }

    @Override
    public int getOrder() {
      // We want to close the channel context after the Hazelcast
      // transaction commits.
      return SynchronizedTransactionalTaskContext.DEFAULT_PRECEDENCE + 1;
    }

    @Override
    public void afterCompletion(int status) {
      super.afterCompletion(status);

      // Cleanup resources.
      channelContext.close();
      TransactionSynchronizationManager.unbindResourceIfPossible(resourceKey);
    }
  }

  private static class ChannelContextInvocationHandler implements
      InvocationHandler {

    private final ChannelContext channelContext;

    public ChannelContextInvocationHandler(ChannelContext channelContext) {
      this.channelContext = channelContext;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws
        Throwable {

      switch (method.getName()) {
        case "close":
          // Ignore close requests because we'll close the context
          // when the transaction is complete.
          return null;

        default:
          return method.invoke(channelContext, args);
      }
    }
  }
}
