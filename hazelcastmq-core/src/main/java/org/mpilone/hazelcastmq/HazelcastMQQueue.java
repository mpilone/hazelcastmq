package org.mpilone.hazelcastmq;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * A queue destination for a HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQQueue implements Queue {

  /**
   * The session that created the queue.
   */
  protected HazelcastMQSession session;

  /**
   * The name of the queue.
   */
  protected String queueName;

  /**
   * Constructs the queue with the given name.
   * 
   * @param session
   *          the parent session
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQQueue(HazelcastMQSession session, String queueName) {
    this.session = session;
    this.queueName = queueName;
  }

  /**
   * Constructs the queue with no session.
   * 
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQQueue(String queueName) {
    this(null, queueName);
  }

  public void setSession(HazelcastMQSession session) {
    this.session = session;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Queue#getQueueName()
   */
  @Override
  public String getQueueName() throws JMSException {
    return queueName;
  }

}
