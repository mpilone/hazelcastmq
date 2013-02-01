package org.mpilone.hazelcastmq;

/**
 * The configuration of the HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQConfig {

  /**
   * The message converter to use for converting JMS messages into and out of
   * Hazelcast. The default is the {@link StompLikeMessageConverter}.
   */
  private MessageConverter messageConverter = new StompLikeMessageConverter();

  /**
   * The maximum number of messages to buffer during topic reception before
   * messages start getting dropped. Choose a value that is a balance between
   * memory usage and consumer performance. This value is per topic consumer.
   * The default is 1000.
   */
  private int topicMaxMessageCount = 1000;

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

}
