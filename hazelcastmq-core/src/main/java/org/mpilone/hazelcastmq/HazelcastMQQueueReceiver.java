package org.mpilone.hazelcastmq;

import javax.jms.*;

import com.hazelcast.core.IQueue;

/**
 * A JMS queue receiver/consumer for HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQQueueReceiver extends HazelcastMQMessageConsumer
    implements QueueReceiver {
  /**
   * The Hazelcast queue from which all messages will be consumed.
   */
  private IQueue<byte[]> hazelcastQueue;

  /**
   * Constructs the receiver whichi will consume from the given queue.
   * 
   * @param session
   *          the parent session
   * @param queue
   *          the queue destination from which to consume
   * @throws JMSException
   */
  public HazelcastMQQueueReceiver(HazelcastMQSession session,
      HazelcastMQQueue queue) throws JMSException {
    super(session, queue);

    hazelcastQueue = hazelcast.getQueue(getQueue().getQueueName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.prss.contentdepot.integration.util.camel.hazelcastjms.
   * HazelcastJmsMessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    setMessageListener(null);

    super.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.prss.contentdepot.integration.util.camel.hazelcastjms.
   * HazelcastJmsMessageConsumer#setMessageListener(javax.jms.MessageListener)
   */
  @Override
  public void setMessageListener(MessageListener messageListener)
      throws JMSException {

    super.setMessageListener(messageListener);

    // If we have a message listener, start polling it.
    if (messageListener != null) {
      startMessageQueuePoller(hazelcastQueue);
    }
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

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive()
   */
  @Override
  public Message receive() throws JMSException {
    return receive(hazelcastQueue, -1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive(long)
   */
  @Override
  public Message receive(long timeout) throws JMSException {
    return receive(hazelcastQueue, timeout);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receiveNoWait()
   */
  @Override
  public Message receiveNoWait() throws JMSException {
    return receive(hazelcastQueue, 0);
  }

}
