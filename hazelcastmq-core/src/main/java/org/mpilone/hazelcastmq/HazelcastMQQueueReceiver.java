package org.mpilone.hazelcastmq;

import javax.jms.*;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;

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
   * The Hazelcast listener used when a JMS {@link MessageListener} has been
   * registered requiring event driven message consumption.
   */
  private ItemListener<byte[]> hazelcastListener;

  /**
   * The flag which indicates if the consumer is currently event driven or
   * polling.
   */
  private boolean eventDriven = false;

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

    hazelcastListener = new ItemListener<byte[]>() {
      @Override
      public void itemAdded(ItemEvent<byte[]> evt) {
        onHazelcastItemAdded();
      }

      @Override
      public void itemRemoved(ItemEvent<byte[]> evt) {
      }
    };
  }

  /**
   * Called when the receiver is event driven and a new message was added to the
   * Hazelcast queue.
   */
  protected void onHazelcastItemAdded() {
    try {
      // Try to get the new message out of the queue.
      Message msg = receiveNoWait();
      if (msg != null && messageListener != null) {
        messageListener.onMessage(msg);
      }
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.prss.contentdepot.integration.util.camel.hazelcastjms.
   * HazelcastJmsMessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    super.close();

    setMessageListener(null);
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

    if (messageListener == null && eventDriven) {
      // Switch to polling consumption.
      hazelcastQueue.removeItemListener(hazelcastListener);
      eventDriven = false;
    }
    else if (messageListener != null && !eventDriven) {
      // Switch to event driven consumption.
      hazelcastQueue.addItemListener(hazelcastListener, false);
      eventDriven = true;
    }
    else {
      // We're already event driven. No change.
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
