package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import java.io.Serializable;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQConsumer;
import org.mpilone.hazelcastmq.core.HazelcastMQContext;
import org.mpilone.hazelcastmq.core.HazelcastMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JMS session in HazelcastMQ.
 * 
 * @author mpilone
 */
class HazelcastMQJmsSession implements Session {

  /**
   * The parent connection.
   */
  private HazelcastMQJmsConnection connection;

  private HazelcastMQJmsConfig config;

  // /**
  // * The ID generator used to generate unique IDs for temporary queues and
  // * topics.
  // */
  // private IdGenerator idGenerator;

  /**
   * The log for this class.
   */
  @SuppressWarnings("unused")
  private final Logger log = LoggerFactory.getLogger(getClass());

  private String id;

  private HazelcastMQContext mqContext;

  /**
   * Constructs the session.
   * 
   * @param connection
   *          the parent connection
   * @param transacted
   *          true for transactional sending, false otherwise
   */
  public HazelcastMQJmsSession(HazelcastMQJmsConnection connection,
      boolean transacted) {
    this.connection = connection;
    this.config = this.connection.getConfig();
    this.id = config.getIdGenerator().newId();
    this.mqContext = config.getHazelcastMQInstance().createContext(transacted);

    mqContext.setAutoStart(false);
  }

  public String getId() {
    return id;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#close()
   */
  @Override
  public void close() throws JMSException {
    stop();

    mqContext.close();

    connection.onSessionClosed(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#commit()
   */
  @Override
  public void commit() throws JMSException {
    mqContext.commit();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createBrowser(javax.jms.Queue)
   */
  @Override
  public QueueBrowser createBrowser(Queue arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
   */
  @Override
  public QueueBrowser createBrowser(Queue arg0, String arg1)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createBytesMessage()
   */
  @Override
  public BytesMessage createBytesMessage() throws JMSException {
    return new HazelcastMQJmsBytesMessage();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createConsumer(javax.jms.Destination)
   */
  @Override
  public MessageConsumer createConsumer(Destination destination)
      throws JMSException {

    HazelcastMQJmsMessageConsumer consumer = null;

    if (destination instanceof HazelcastMQJmsTopic) {
      HazelcastMQJmsTopic topic = (HazelcastMQJmsTopic) destination;
      HazelcastMQConsumer mqConsumer = mqContext.createConsumer(topic
          .getMqName());

      consumer = new HazelcastMQJmsTopicSubscriber(mqConsumer, this, topic);
    }
    else {
      HazelcastMQJmsQueue queue = (HazelcastMQJmsQueue) destination;
      HazelcastMQConsumer mqConsumer = mqContext.createConsumer(queue
          .getMqName());

      consumer = new HazelcastMQJmsQueueReceiver(mqConsumer, this, queue);
    }

    return consumer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createConsumer(javax.jms.Destination,
   * java.lang.String)
   */
  @Override
  public MessageConsumer createConsumer(Destination destination,
      String messageSelector) throws JMSException {

    if (messageSelector != null) {
      throw new UnsupportedOperationException(format(
          "Creating consumer with selector [%s] but "
              + "selectors are not currently supported.", messageSelector));
    }

    return createConsumer(destination);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createConsumer(javax.jms.Destination,
   * java.lang.String, boolean)
   */
  @Override
  public MessageConsumer createConsumer(Destination destination,
      String messageSelector, boolean noLocal) throws JMSException {
    if (noLocal) {
      throw new UnsupportedOperationException(
          "Creating consumer with noLocal option but "
              + "noLocal is not currently supported.");
    }

    return createConsumer(destination, messageSelector);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic,
   * java.lang.String)
   */
  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String arg1)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic,
   * java.lang.String, java.lang.String, boolean)
   */
  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String arg1,
      String arg2, boolean arg3) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createMapMessage()
   */
  @Override
  public MapMessage createMapMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createMessage()
   */
  @Override
  public Message createMessage() throws JMSException {
    return new HazelcastMQJmsMessage();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createObjectMessage()
   */
  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createObjectMessage(java.io.Serializable)
   */
  @Override
  public ObjectMessage createObjectMessage(Serializable arg0)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createProducer(javax.jms.Destination)
   */
  @Override
  public MessageProducer createProducer(Destination destination)
      throws JMSException {
    HazelcastMQProducer mqProducer = mqContext.createProducer();

    HazelcastMQJmsMessageProducer producer = new HazelcastMQJmsMessageProducer(
        mqProducer, this, destination);
    return producer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createQueue(java.lang.String)
   */
  @Override
  public Queue createQueue(String queueName) throws JMSException {
    return new HazelcastMQJmsQueue(queueName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createStreamMessage()
   */
  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTemporaryQueue()
   */
  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    return connection.createTemporaryQueue();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTemporaryTopic()
   */
  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    return connection.createTemporaryTopic();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTextMessage()
   */
  @Override
  public TextMessage createTextMessage() throws JMSException {
    return new HazelcastMQJmsTextMessage();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTextMessage(java.lang.String)
   */
  @Override
  public TextMessage createTextMessage(String text) throws JMSException {
    TextMessage msg = createTextMessage();
    msg.setText(text);
    return msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTopic(java.lang.String)
   */
  @Override
  public Topic createTopic(String topicName) throws JMSException {
    return new HazelcastMQJmsTopic(topicName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#getAcknowledgeMode()
   */
  @Override
  public int getAcknowledgeMode() throws JMSException {
    if (getTransacted()) {
      return SESSION_TRANSACTED;
    }
    else {
      return AUTO_ACKNOWLEDGE;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#getMessageListener()
   */
  @Override
  public MessageListener getMessageListener() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#getTransacted()
   */
  @Override
  public boolean getTransacted() throws JMSException {
    return mqContext.isTransacted();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#recover()
   */
  @Override
  public void recover() throws JMSException {
    // no op
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#rollback()
   */
  @Override
  public void rollback() throws JMSException {
    mqContext.rollback();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#run()
   */
  @Override
  public void run() {
    // no op
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#setMessageListener(javax.jms.MessageListener)
   */
  @Override
  public void setMessageListener(MessageListener arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#unsubscribe(java.lang.String)
   */
  @Override
  public void unsubscribe(String arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  HazelcastMQJmsConfig getConfig() {
    return connection.getConfig();
  }

  /**
   * Returns the parent connection of this session.
   * 
   * @return the parent connection
   */
  HazelcastMQJmsConnection getConnection() {
    return connection;
  }

  /**
   * Starts this session. This method must be called by the connection when it
   * is started.
   */
  void start() {
    mqContext.start();
  }

  /**
   * Stops this session. This method must be called by the connection when it is
   * stopped.
   */
  void stop() {
    mqContext.stop();
  }

}
