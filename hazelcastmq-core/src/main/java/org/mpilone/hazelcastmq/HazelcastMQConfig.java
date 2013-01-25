package org.mpilone.hazelcastmq;

/**
 * The configuration of the HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQConfig {

  /**
   * The message marshaller to use for marshalling JMS messages into and out of
   * Hazelcast. The default is the {@link StompLikeMessageMarshaller}.
   */
  private MessageMarshaller messageMarshaller = new StompLikeMessageMarshaller();

  /**
   * The maximum number of messages to buffer during topic reception before
   * messages start getting dropped. Choose a value that is a balance between
   * memory usage and consumer performance. This value is per topic consumer.
   * The default is 1000.
   */
  private int topicMaxMessageCount = 1000;

  /**
   * Returns the message marshaller to use for marshalling JMS messages into and
   * out of Hazelcast. The default is the {@link StompLikeMessageMarshaller}.
   * 
   * @return the messageMarshaller the message marshaller
   */
  public MessageMarshaller getMessageMarshaller() {
    return messageMarshaller;
  }

  /**
   * @param messageMarshaller
   *          the message marshaller
   */
  public void setMessageMarshaller(MessageMarshaller messageMarshaller) {
    this.messageMarshaller = messageMarshaller;
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
