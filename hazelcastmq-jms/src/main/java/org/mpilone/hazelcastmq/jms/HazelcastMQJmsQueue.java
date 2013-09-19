package org.mpilone.hazelcastmq.jms;

import javax.jms.JMSException;
import javax.jms.Queue;

import org.mpilone.hazelcastmq.core.Headers;

/**
 * A queue destination for a HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsQueue extends HazelcastMQJmsDestination implements Queue {

  /**
   * Constructs the queue with the given name.
   * 
   * @param session
   *          the parent session
   * @param queueName
   *          the name of the queue
   */
  protected HazelcastMQJmsQueue(String destinationPrefix, String queueName) {
    super(destinationPrefix, queueName);
  }

  /**
   * Constructs the queue with no session.
   * 
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQJmsQueue(String queueName) {
    this(Headers.DESTINATION_QUEUE_PREFIX, queueName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Queue#getQueueName()
   */
  @Override
  public String getQueueName() throws JMSException {
    return getDestinationName();
  }
}
