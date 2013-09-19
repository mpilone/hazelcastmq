package org.mpilone.hazelcastmq.jms;

import javax.jms.JMSException;
import javax.jms.Topic;

import org.mpilone.hazelcastmq.core.Headers;

/**
 * A JMS topic for HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsTopic extends HazelcastMQJmsDestination implements Topic {

  /**
   * Constructs the topic with no session.
   * 
   * @param topicName
   *          the name of the topic
   */
  public HazelcastMQJmsTopic(String topicName) {
    this(Headers.DESTINATION_TOPIC_PREFIX, topicName);
  }

  protected HazelcastMQJmsTopic(String destinationPrefix, String topicName) {
    super(destinationPrefix, topicName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Topic#getTopicName()
   */
  @Override
  public String getTopicName() throws JMSException {
    return getDestinationName();
  }

}
