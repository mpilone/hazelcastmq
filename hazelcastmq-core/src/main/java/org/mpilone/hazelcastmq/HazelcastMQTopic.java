package org.mpilone.hazelcastmq;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * A JMS topic for HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQTopic implements Topic {

  /**
   * The parent session.
   */
  protected HazelcastMQSession session;

  /**
   * The topic name.
   */
  protected String topicName;

  /**
   * Constructs a topic with the given name.
   * 
   * @param session
   *          the parent session
   * @param topicName
   *          the topic name
   */
  public HazelcastMQTopic(HazelcastMQSession session, String topicName) {
    this.session = session;
    this.topicName = topicName;
  }

  /**
   * Constructs the topic with no session.
   * 
   * @param topicName
   *          the name of the topic
   */
  public HazelcastMQTopic(String topicName) {
    this(null, topicName);
  }

  /**
   * Sets the parent session of this topic. Normally this method is not needed
   * because the session should be set via the constructor.
   * 
   * @param session
   *          the parent session
   */
  public void setSession(HazelcastMQSession session) {
    this.session = session;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Topic#getTopicName()
   */
  @Override
  public String getTopicName() throws JMSException {
    return topicName;
  }

}
