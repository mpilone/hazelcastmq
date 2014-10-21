package org.mpilone.hazelcastmq.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * The configuration of the HazelcastMQ instance.
 * 
 * @author mpilone
 */
public class HazelcastMQConfig {

  private HazelcastInstance hazelcastInstance;
  private MessageConverter messageConverter = new NoOpMessageConverter();
  private int topicMaxMessageCount = 1000;
  private ExecutorService executor;
  private ContextDispatchStrategy contextDispatchStrategy =
      ContextDispatchStrategy.DEDICATED_THREAD;

  /**
   * Constructs the configuration with the following defaults:
   * <ul>
   * <li>messageConverter: {@link NoOpMessageConverter}</li>
   * <li>topicMaxMessageCount: 1000</li>
   * <li>executor: {@link Executors#newCachedThreadPool()} (lazy initialized)</li>
   * <li>hazelcastInstance: {@link Hazelcast#newHazelcastInstance()} (lazy
   * initialized)</li>
   * <li>contextDispatchStrategy:
   * {@link ContextDispatchStrategy#DEDICATED_THREAD}</li>
   * </ul>
   */
  public HazelcastMQConfig() {
  }

  /**
   * Constructs the configuration with the following defaults:
   * <ul>
   * <li>messageConverter: {@link NoOpMessageConverter}</li>
   * <li>topicMaxMessageCount: 1000</li>
   * <li>executor: {@link Executors#newCachedThreadPool()} (lazy
   * initialized)</li>
   * <li>contextDispatchStrategy:
   * {@link ContextDispatchStrategy#DEDICATED_THREAD}</li>
   * </ul>
   *
   * @param hzInstance the Hazelcast instances to use for all queues and topic
   * operations
   */
  public HazelcastMQConfig(HazelcastInstance hzInstance) {
    setHazelcastInstance(hzInstance);
  }

  /**
   * Sets the context dispatch strategy used to dispatch messages to the
   * consumers within a single {@link HazelcastMQContext}. Be sure to read the
   * trade-offs of each strategy in the {@link ContextDispatchStrategy}
   * documentation. The strategy selected may impact the desired configuration
   * of the {@link #getExecutor() executor} to control dispatch concurrency and
   * resource usage. The default is
   * {@link ContextDispatchStrategy#DEDICATED_THREAD}.
   *
   * @param contextDispatchStrategy the dispatch strategy to use in contexts
   */
  public void setContextDispatchStrategy(
      ContextDispatchStrategy contextDispatchStrategy) {
    this.contextDispatchStrategy = contextDispatchStrategy;
  }

  /**
   * Returns the context dispatch strategy used to dispatch messages to the
   * consumers within a single {@link HazelcastMQContext}. Be sure to read the
   * trade-offs of each strategy in the {@link ContextDispatchStrategy}
   * documentation.
   *
   * @return the dispatch strategy to use in contexts
   */
  public ContextDispatchStrategy getContextDispatchStrategy() {
    return contextDispatchStrategy;
  }

  /**
   * Returns the message converter to use for converter HazelcastMQ messages
   * into and out of the objects/data sent into Hazelcast. The default is the
   * {@link NoOpMessageConverter} so the original {@link HazelcastMQMessage} is
   * simply passed through to Hazelcast for internal serialization.
    * 
   * @return the message converter
   */
  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

  /**
   * Sets the message converter to use for converter HazelcastMQ messages into
   * and out of the objects/data sent into Hazelcast.
   *
   * @param messageConverter the message converter
   */
  public void setMessageConverter(MessageConverter messageConverter) {
    this.messageConverter = messageConverter;
  }

  /**
   * Returns the maximum number of messages to buffer during topic reception
   * before messages start getting dropped. Choose a value that is a balance
   * between memory usage and consumer performance. This value is per topic
   * consumer. The default is 1000.
   * 
   * @return the topicMaxMessageCount
   */
  public int getTopicMaxMessageCount() {
    return topicMaxMessageCount;
  }

  /**
   * @param topicMaxMessageCount
   *          the maximum number of topic messages to buffer
   */
  public void setTopicMaxMessageCount(int topicMaxMessageCount) {
    this.topicMaxMessageCount = topicMaxMessageCount;
  }

  /**
   * Returns the executor that will be used to create message consumer threads
   * when a message listener is active.
   * 
   * @return the executor service
   */
  public ExecutorService getExecutor() {
    if (executor == null) {
      executor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicLong counter = new AtomicLong();
        private final ThreadFactory delegate = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
          Thread t = delegate.newThread(r);
          t.setName("hazelcastmq-" + counter.incrementAndGet());
          t.setDaemon(true);
          return t;
        }
      });
    }

    return executor;
  }

  /**
   * Sets the executor to use for all background threads.
   * 
   * @param executor
   *          the executor instance
   */
  public void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Returns the Hazelcast instance that this MQ will use for all queue and
   * topic operations.
   * 
   * @return the Hazelcast instance
   */
  public HazelcastInstance getHazelcastInstance() {
    if (hazelcastInstance == null) {
      hazelcastInstance = Hazelcast.newHazelcastInstance();
    }
    return hazelcastInstance;
  }

  /**
   * Sets the Hazelcast instance that this MQ will use for all queue and topic
   * operations.
   * 
   * @param hazelcastInstance
   *          the Hazelcast instance
   */
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * The dispatch strategy to use within a single {@link HazelcastMQContext}.
   */
  public enum ContextDispatchStrategy {

    /**
     * A dedicated thread dispatcher that uses a tight loop and a lock condition
     * to sleep until signaled. The thread, allocated from the thread pool in
     * the constructor, is dedicated to this dispatcher until it is closed. This
     * has the benefit of an almost zero latency between the signal and the
     * dispatch; however with a large number of dedicated threads are required
     * if a large number of contexts are created. It is important to use a
     * proper thread pool configuration to be sure that every context can
     * allocate a thread (i.e. an unbounded thread pool) or a context may never
     * dispatch messages to consumers.
     */
    DEDICATED_THREAD,
    /**
     * A single shot dispatcher that makes the trade-off of some latency for a
     * lower overall thread count. Each time the dispatcher is signalled it adds
     * itself to the thread pool executor for eventual dispatch. This has the
     * benefit of using few (potentially only 1) thread based on the thread poll
     * configuration; however there is an extra cost in the thread pool to
     * allocate a thread and run the dispatcher. The number of concurrent
     * contexts (and therefore the number of concurrent consumers across
     * contexts) will be limited by the thread pool configuration.
     */
    REACTOR
  }

}
