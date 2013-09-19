package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

import org.mpilone.hazelcastmq.core.Headers;

/**
 * A JMS temporary topic for HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsTemporaryTopic extends HazelcastMQJmsTopic implements
    TemporaryTopic {

  /**
   * The parent session.
   */
  protected HazelcastMQJmsConnection connection;

  /**
   * Constructs a topic with the given name.
   * 
   * @param session
   *          the parent session
   * @param topicName
   *          the topic name
   */
  public HazelcastMQJmsTemporaryTopic(HazelcastMQJmsConnection connection,
      String topicName) {
    super(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX, topicName);
    this.connection = connection;
  }

  /**
   * Constructs the topic with no session. Attempting to delete a topic with no
   * parent session will result in an exception.
   * 
   * @param topicName
   *          the name of the topic
   */
  public HazelcastMQJmsTemporaryTopic(String topicName) {
    this(null, topicName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TemporaryTopic#delete()
   */
  @Override
  public void delete() throws JMSException {
    if (connection == null) {
      throw new IllegalStateException(format(
          "Cannot delete topic [%s] because it is "
              + "not associated with a connection in this instance.",
          getDestinationName()));
    }

    connection.deleteTemporaryDestination(this);
  }
}
