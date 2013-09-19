package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQMessage;
import org.mpilone.hazelcastmq.core.HazelcastMQProducer;

/**
 * A JMS message producer which sends message to a HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsMessageProducer implements MessageProducer {

  /**
   * The parent session.
   */
  private HazelcastMQJmsSession session;

  /**
   * The default destination to which to send.
   */
  private Destination destination;

  /**
   * The ID generator used to create unique message and correlation IDs.
   */
  private IdGenerator idGenerator;

  /**
   * The message marshaller to marshal to and from Hazelcast.
   */
  private MessageConverter messageMarshaller;

  /**
   * The default delivery mode. Not currently supported.
   */
  private int deliveryMode = DeliveryMode.NON_PERSISTENT;

  /**
   * The flag to disable message ID generation. Defaults to false.
   */
  private boolean disableMessageID = false;

  /**
   * The flag to disable message timestamp generation. Defaults to false.
   */
  private boolean disableMessageTimestamp = false;

  /**
   * The default message priority. Defaults to 4.
   */
  private int priority = 4;

  /**
   * The default message time to live. Defaults to 0 (forever).
   */
  private long timeToLive = 0;

  /**
   * The header value prefix used when generating message and correlation IDs.
   */
  private static final String HEADER_ID_PREFIX = "hazelcastMQ.id";

  private HazelcastMQProducer mqProducer;

  /**
   * Constructs the producer which will send to the given destination.
   * 
   * @param session
   *          the parent JMS session
   * @param destination
   *          the destination to which to send messages
   */
  public HazelcastMQJmsMessageProducer(HazelcastMQProducer mqProducer,
      HazelcastMQJmsSession session, Destination destination) {
    this.session = session;
    this.destination = destination;
    this.mqProducer = mqProducer;

    this.idGenerator = this.session.getConfig().getIdGenerator();
    this.messageMarshaller = this.session.getConfig().getMessageConverter();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#close()
   */
  @Override
  public void close() throws JMSException {
    // no op
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getDeliveryMode()
   */
  @Override
  public int getDeliveryMode() throws JMSException {
    return deliveryMode;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getDestination()
   */
  @Override
  public Destination getDestination() throws JMSException {
    return destination;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getDisableMessageID()
   */
  @Override
  public boolean getDisableMessageID() throws JMSException {
    return disableMessageID;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getDisableMessageTimestamp()
   */
  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    return disableMessageTimestamp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getPriority()
   */
  @Override
  public int getPriority() throws JMSException {
    return priority;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#getTimeToLive()
   */
  @Override
  public long getTimeToLive() throws JMSException {
    return timeToLive;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#send(javax.jms.Message)
   */
  @Override
  public void send(Message msg) throws JMSException {
    send(destination, msg, deliveryMode, priority, timeToLive);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#send(javax.jms.Destination,
   * javax.jms.Message)
   */
  @Override
  public void send(Destination destination, Message msg) throws JMSException {
    send(destination, msg, deliveryMode, priority, timeToLive);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#send(javax.jms.Message, int, int, long)
   */
  @Override
  public void send(Message msg, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
    send(destination, msg, deliveryMode, priority, timeToLive);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#send(javax.jms.Destination,
   * javax.jms.Message, int, int, long)
   */
  @Override
  public void send(Destination destination, Message msg, int deliveryMode,
      int priority, long timeToLive) throws JMSException {

    if (!(destination instanceof HazelcastMQJmsQueue)
        && !(destination instanceof HazelcastMQJmsTopic)) {
      throw new InvalidDestinationException(format(
          "The destination [%s] is not a valid Hazelcast JMS destination.",
          destination));
    }

    long now = System.currentTimeMillis();

    msg.setJMSDeliveryMode(deliveryMode);
    msg.setJMSPriority(priority);
    msg.setJMSExpiration(timeToLive == 0 ? 0 : now + timeToLive);

    if (msg.getJMSCorrelationID() == null) {
      msg.setJMSCorrelationID(HEADER_ID_PREFIX + ".correlation."
          + idGenerator.newId());
    }
    if (!disableMessageID) {
      msg.setJMSMessageID(HEADER_ID_PREFIX + ".message." + idGenerator.newId());
    }
    if (!disableMessageTimestamp) {
      msg.setJMSTimestamp(now);
    }

    HazelcastMQJmsDestination jmsDest = (HazelcastMQJmsDestination) destination;
    HazelcastMQMessage mqMsg = messageMarshaller.fromJmsMessage(msg);

    try {
      mqProducer.setTimeToLive(timeToLive);
      mqProducer.send(jmsDest.getMqName(), mqMsg);
    }
    catch (Exception ex) {
      throw new JMSException("Unable to send message via HazelcastMQ: "
          + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#setDeliveryMode(int)
   */
  @Override
  public void setDeliveryMode(int deliveryMode) throws JMSException {
    this.deliveryMode = deliveryMode;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#setDisableMessageID(boolean)
   */
  @Override
  public void setDisableMessageID(boolean disableMessageID) throws JMSException {
    this.disableMessageID = disableMessageID;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#setDisableMessageTimestamp(boolean)
   */
  @Override
  public void setDisableMessageTimestamp(boolean disableMessageTimestamp)
      throws JMSException {
    this.disableMessageTimestamp = disableMessageTimestamp;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#setPriority(int)
   */
  @Override
  public void setPriority(int priority) throws JMSException {
    this.priority = priority;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageProducer#setTimeToLive(long)
   */
  @Override
  public void setTimeToLive(long timeToLive) throws JMSException {
    this.timeToLive = timeToLive;
  }

}
