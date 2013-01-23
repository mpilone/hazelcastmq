package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;

/**
 * A queue destination for a HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQQueue implements Queue, TemporaryQueue {

  /**
   * The session that created the queue.
   */
  private HazelcastMQSession session;

  /**
   * The name of the queue.
   */
  private String queueName;

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
   * Constructs the queue with no session. Attempting to delete a queue with no
   * parent session will result in an exception.
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

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TemporaryQueue#delete()
   */
  @Override
  public void delete() throws JMSException {
    if (session == null) {
      throw new IllegalStateException(format(
          "Cannot delete queue [%s] because it is "
              + "not associated with a session in this instance.", queueName));
    }

    session.getHazelcast().getQueue(queueName).destroy();
  }

}
