package org.mpilone.hazelcastmq.jms;

import java.util.concurrent.TimeUnit;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.jms.MessageConverter;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A JMS message consumer that can consume from a HazelcastMQ queue or topic.
 * 
 * @author mpilone
 */
abstract class HazelcastMQJmsMessageConsumer implements MessageConsumer {

  private class ConvertingMessageListener implements HazelcastMQMessageListener {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.HazelcastMQMessageHandler#handle(org.mpilone
     * .hazelcastmq.core.HazelcastMQMessage)
     */
    @Override
    public void onMessage(HazelcastMQMessage mqMsg) {

      try {
        Message msg = messageMarshaller.toJmsMessage(mqMsg);

        if (messageListener != null) {
          messageListener.onMessage(msg);
        }
      }
      catch (Throwable ex) {
        // TODO
      }
    }
  }

  /**
   * The log for this class.
   */
  private final ILogger log = Logger.getLogger(getClass());

  /**
   * The parent session.
   */
  protected HazelcastMQJmsSession session;

  /**
   * The JMS destination from which to consume.
   */
  protected Destination destination;

  /**
   * The message listener that will be notified of new messages as they arrive.
   */
  protected MessageListener messageListener;

  /**
   * The message marshaller used to marshal messages in and out of HazelcastMQ.
   */
  protected MessageConverter messageMarshaller;

  private HazelcastMQConsumer mqConsumer;

  private String messageSelector;

  /**
   * Constructs the consumer which will consume from the given destination.
   * 
   * @param session
   *          the parent session
   * @param destination
   *          the destination from which to consume
   * @throws JMSException
   */
  public HazelcastMQJmsMessageConsumer(HazelcastMQConsumer mqConsumer,
      HazelcastMQJmsSession session, Destination destination)
      throws JMSException {
    this(mqConsumer, session, destination, null);
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
  public HazelcastMQJmsMessageConsumer(HazelcastMQConsumer mqConsumer,
      HazelcastMQJmsSession session, Destination destination,
      String messageSelector) throws JMSException {
    this.session = session;
    this.destination = destination;
    this.messageSelector = messageSelector;

    this.messageMarshaller = this.session.getConfig().getMessageConverter();
    this.mqConsumer = mqConsumer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    mqConsumer.close();
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

    if (messageListener == null) {
      mqConsumer.setMessageListener(null);
    }
    else {
      mqConsumer.setMessageListener(new ConvertingMessageListener());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive(long)
   */
  @Override
  public Message receive(long timeout) throws JMSException {
    HazelcastMQMessage mqMsg = mqConsumer.receive(timeout,
        TimeUnit.MILLISECONDS);

    if (mqMsg != null) {
      return messageMarshaller.toJmsMessage(mqMsg);
    }
    else {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receiveNoWait()
   */
  @Override
  public Message receiveNoWait() throws JMSException {
    HazelcastMQMessage mqMsg = mqConsumer.receiveNoWait();

    if (mqMsg != null) {
      return messageMarshaller.toJmsMessage(mqMsg);
    }
    else {
      return null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive()
   */
  @Override
  public Message receive() throws JMSException {
    HazelcastMQMessage mqMsg = mqConsumer.receive();

    if (mqMsg != null) {
      return messageMarshaller.toJmsMessage(mqMsg);
    }
    else {
      return null;
    }
  }
}
