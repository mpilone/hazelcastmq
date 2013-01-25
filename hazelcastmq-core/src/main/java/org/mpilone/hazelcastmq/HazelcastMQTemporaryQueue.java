package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

/**
 * A temporary queue destination for a HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQTemporaryQueue extends HazelcastMQQueue implements
    TemporaryQueue {

  /**
   * Constructs the queue with the given name.
   * 
   * @param session
   *          the parent session
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQTemporaryQueue(HazelcastMQSession session, String queueName) {
    super(session, queueName);
  }

  /**
   * Constructs the queue with no session. Attempting to delete a queue with no
   * parent session will result in an exception.
   * 
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQTemporaryQueue(String queueName) {
    this(null, queueName);
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

    session.getConnection().deleteTemporaryDestination(this);
  }
}
