package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

import org.mpilone.hazelcastmq.core.Headers;

/**
 * A temporary queue destination for a HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsTemporaryQueue extends HazelcastMQJmsQueue implements
    TemporaryQueue {

  /**
   * The connection that created the queue.
   */
  protected HazelcastMQJmsConnection connection;

  /**
   * Constructs the queue with the given name.
   * 
   * @param session
   *          the parent session
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQJmsTemporaryQueue(HazelcastMQJmsConnection connection,
      String queueName) {
    super(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX, queueName);

    this.connection = connection;
  }

  /**
   * Constructs the queue with no session. Attempting to delete a queue with no
   * parent session will result in an exception.
   * 
   * @param queueName
   *          the name of the queue
   */
  public HazelcastMQJmsTemporaryQueue(String queueName) {
    this(null, queueName);
  }

  // /**
  // * Sets the parent session of this topic. Normally this method is not needed
  // * because the session should be set via the constructor.
  // *
  // * @param session
  // * the parent session
  // */
  // public void setSession(HazelcastMQJmsSession session) {
  // this.session = session;
  // }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TemporaryQueue#delete()
   */
  @Override
  public void delete() throws JMSException {
    if (connection == null) {
      throw new IllegalStateException(format(
          "Cannot delete queue [%s] because it is "
              + "not associated with a connection in this instance.",
          getQueueName()));
    }

    connection.deleteTemporaryDestination(this);
  }
}
