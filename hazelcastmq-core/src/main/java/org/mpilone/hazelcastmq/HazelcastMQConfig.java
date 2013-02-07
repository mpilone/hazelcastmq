package org.mpilone.hazelcastmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The configuration of the HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQConfig {

  /**
   * The message converter to use for converting JMS messages into and out of
   * Hazelcast.
   */
  private MessageConverter messageConverter = new StompLikeMessageConverter();

  /**
   * The maximum number of messages to buffer during topic reception before
   * messages start getting dropped. Choose a value that is a balance between
   * memory usage and consumer performance. This value is per topic consumer.
   */
  private int topicMaxMessageCount;

  /**
   * The executor service to spin up message listener consumers.
   */
  private ExecutorService executor;

  /**
   * Constructs the configuration with the following defaults:
   * <ul>
   * <li>messageConverter: {@link StompLikeMessageConverter}</li>
   * <li>topicMaxMessageCount: 1000</li>
   * <li>executor: {@link Executors#newCachedThreadPool()}</li>
   * </ul>
   */
  public HazelcastMQConfig() {
    messageConverter = new StompLikeMessageConverter();
    topicMaxMessageCount = 1000;
    executor = Executors.newCachedThreadPool(new ThreadFactory() {

      private AtomicLong counter = new AtomicLong();
      private ThreadFactory delegate = Executors.defaultThreadFactory();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = delegate.newThread(r);
        t.setName("hazelcastmq-" + counter.incrementAndGet());
        t.setDaemon(true);
        return t;
      }
    });
  }

  /**
   * Returns the message marshaller to use for marshalling JMS messages into and
   * out of Hazelcast. The default is the {@link StompLikeMessageConverter}.
   * 
   * @return the messageMarshaller the message marshaller
   */
  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

  /**
   * @param messageMarshaller
   *          the message marshaller
   */
  public void setMessageConverter(MessageConverter messageMarshaller) {
    this.messageConverter = messageMarshaller;
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
    return executor;
  }

  /**
   * 
   * @param executor
   */
  public void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

}
