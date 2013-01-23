package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

/**
 * A JMS topic for HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQTopic implements Topic, TemporaryTopic {

  /**
   * The parent session.
   */
  private HazelcastMQSession session;

  /**
   * The topic name.
   */
  private String topicName;

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
   * Constructs the topic with no session. Attempting to delete a topic with no
   * parent session will result in an exception.
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

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TemporaryTopic#delete()
   */
  @Override
  public void delete() throws JMSException {
    if (session == null) {
      throw new IllegalStateException(format(
          "Cannot delete topic [%s] because it is "
              + "not associated with a session in this instance.", topicName));
    }

    session.getHazelcast().getTopic(topicName).destroy();
  }

}
