package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * A JMS temporary topic for HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQTemporaryTopic extends HazelcastMQTopic implements
    TemporaryTopic {

  /**
   * Constructs a topic with the given name.
   * 
   * @param session
   *          the parent session
   * @param topicName
   *          the topic name
   */
  public HazelcastMQTemporaryTopic(HazelcastMQSession session, String topicName) {
    super(session, topicName);
  }

  /**
   * Constructs the topic with no session. Attempting to delete a topic with no
   * parent session will result in an exception.
   * 
   * @param topicName
   *          the name of the topic
   */
  public HazelcastMQTemporaryTopic(String topicName) {
    super(topicName);
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

    session.getConnection().deleteTemporaryDestination(this);
  }
}
