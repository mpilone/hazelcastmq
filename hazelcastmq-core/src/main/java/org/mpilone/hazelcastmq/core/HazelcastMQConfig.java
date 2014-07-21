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

  /**
   * The Hazelcast instance to use for all topic and queue management.
   */
  private HazelcastInstance hazelcastInstance;

  /**
   * The message converter to use for converting HazelcastMQ messages into and
   * out of Hazelcast.
   */
  private MessageConverter messageConverter = new NoOpMessageConverter();

  /**
   * The maximum number of messages to buffer during topic reception before
   * messages start getting dropped. Choose a value that is a balance between
   * memory usage and consumer performance. This value is per topic consumer.
   */
  private int topicMaxMessageCount = 1000;

  /**
   * The executor service to spin up message listener consumers.
   */
  private ExecutorService executor;

  /**
   * Constructs the configuration with the following defaults:
   * <ul>
   * <li>messageConverter: {@link NoOpMessageConverter}</li>
   * <li>topicMaxMessageCount: 1000</li>
   * <li>executor: {@link Executors#newCachedThreadPool()} (lazy initialized)</li>
   * <li>hazelcastInstance: {@link Hazelcast#newHazelcastInstance()} (lazy
   * initialized)</li>
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
   * </ul>
   *
   * @param hzInstance the Hazelcast instances to use for all queues and topic
   * operations
   */
  public HazelcastMQConfig(HazelcastInstance hzInstance) {
    setHazelcastInstance(hzInstance);
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

}
