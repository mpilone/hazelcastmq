package org.mpilone.hazelcastmq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import com.hazelcast.core.HazelcastInstance;

/**
 * A JMS message consumer that can consume from a HazelcastMQ queue or topic.
 * 
 * @author mpilone
 */
public abstract class HazelcastMQMessageConsumer implements MessageConsumer {

  /**
   * The parent session.
   */
  protected HazelcastMQSession session;

  /**
   * The JMS destination from which to consume.
   */
  protected Destination destination;

  /**
   * The Hazelcast instance from which to consume.
   */
  protected HazelcastInstance hazelcast;

  /**
   * The message listener that will be notified of new messages as they arrive.
   */
  protected MessageListener messageListener;

  /**
   * The JMS message selector. Currently not used or supported.
   */
  private String messageSelector;

  /**
   * The message marshaller used to marshal messages in and out of HazelcastMQ.
   */
  protected MessageMarshaller messageMarshaller;

  /**
   * Constructs the consumer which will consume from the given destination.
   * 
   * @param session
   *          the parent session
   * @param destination
   *          the destination from which to consume
   * @throws JMSException
   */
  public HazelcastMQMessageConsumer(HazelcastMQSession session,
      Destination destination) throws JMSException {
    this(session, destination, null);
  }

  /**
   * Constructs the consumer which will consume from the given destination.
   * 
   * @param session
   *          the parent session
   * @param destination
   *          the destination from which to consume
   * @param messageSelector
   *          the message selector to filter incoming messages (currently not
   *          supported)
   * @throws JMSException
   */
  public HazelcastMQMessageConsumer(HazelcastMQSession session,
      Destination destination, String messageSelector) throws JMSException {
    this.session = session;
    this.destination = destination;
    this.messageSelector = messageSelector;

    this.messageMarshaller = new StompLikeMessageMarshaller();
    this.hazelcast = session.getHazelcast();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    // no op
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#getMessageListener()
   */
  @Override
  public MessageListener getMessageListener() throws JMSException {
    return messageListener;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#getMessageSelector()
   */
  @Override
  public String getMessageSelector() throws JMSException {
    return messageSelector;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
   */
  @Override
  public void setMessageListener(MessageListener messageListener)
      throws JMSException {
    this.messageListener = messageListener;
  }

  /**
   * Receives a message from the given blocking queue, waiting up to the given
   * timeout value.
   * 
   * @param queue
   *          the queue from which to consume
   * @param timeout
   *          greater than 0 to wait the given number of milliseconds, 0 for
   *          unlimited blocking wait, less than zero for no wait
   * @return the message received or null if no message was received
   * @throws JMSException
   */
  protected Message receive(BlockingQueue<byte[]> queue, long timeout)
      throws JMSException {
    try {
      byte[] msgData = null;
      if (timeout < 0) {
        msgData = queue.take();
      }
      else if (timeout == 0) {
        msgData = queue.poll();
      }
      else {
        msgData = queue.poll(timeout, TimeUnit.MILLISECONDS);
      }

      if (msgData != null) {
        return messageMarshaller.unmarshal(msgData);
      }
      else {
        return null;
      }
    }
    catch (Exception ex) {
      throw new JMSException("Error receiving message: " + ex.getMessage());
    }
  }

}
