package org.mpilone.hazelcastmq.jms;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.mpilone.hazelcastmq.core.HazelcastMQConsumer;

/**
 * A JMS topic subscriber for a HazecastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsTopicSubscriber extends HazelcastMQJmsMessageConsumer
    implements TopicSubscriber {

  /**
   * Constructs the subscriber on the given topic.
   * 
   * @param session
   *          the parent session
   * @param topic
   *          the topic from which to consume
   * @throws JMSException
   */
  public HazelcastMQJmsTopicSubscriber(HazelcastMQConsumer mqConsumer,
      HazelcastMQJmsSession session, HazelcastMQJmsTopic topic)
      throws JMSException {
    super(mqConsumer, session, topic);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TopicSubscriber#getNoLocal()
   */
  @Override
  public boolean getNoLocal() throws JMSException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TopicSubscriber#getTopic()
   */
  @Override
  public Topic getTopic() throws JMSException {
    return (Topic) destination;
  }
}
