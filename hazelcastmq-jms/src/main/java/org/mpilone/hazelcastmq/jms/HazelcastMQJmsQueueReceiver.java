package org.mpilone.hazelcastmq.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import org.mpilone.hazelcastmq.core.HazelcastMQConsumer;

/**
 * A JMS queue receiver/consumer for HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsQueueReceiver extends HazelcastMQJmsMessageConsumer
    implements QueueReceiver {

  /**
   * Constructs the receiver whichi will consume from the given queue.
   * 
   * @param session
   *          the parent session
   * @param queue
   *          the queue destination from which to consume
   * @throws JMSException
   */
  public HazelcastMQJmsQueueReceiver(HazelcastMQConsumer mqConsumer,
      HazelcastMQJmsSession session, HazelcastMQJmsQueue queue)
      throws JMSException {
    super(mqConsumer, session, queue);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.QueueReceiver#getQueue()
   */
  @Override
  public Queue getQueue() throws JMSException {
    return (Queue) destination;
  }
}
