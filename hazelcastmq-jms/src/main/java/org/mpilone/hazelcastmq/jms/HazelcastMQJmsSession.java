package org.mpilone.hazelcastmq.jms;

import static java.lang.String.format;

import java.io.Serializable;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.*;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A JMS session in HazelcastMQ. A JMS session roughly maps to a
 * {@link HazelcastMQContext} and delegates all operations to the context.
  *
 * @author mpilone
 */
class HazelcastMQJmsSession implements Session {

  /**
   * The parent connection.
   */
  private final HazelcastMQJmsConnection connection;

  /**
   * The log for this class.
   */
  @SuppressWarnings("unused")
  private final ILogger log = Logger.getLogger(getClass());

  /**
   * The ID of this session in the connection.
   */
  private final String id;

  /**
   * The MQ context backing the session and connection.
   */
  private final HazelcastMQContext mqContext;

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

    HazelcastMQJmsConfig config = this.connection.getConfig();
    this.id = config.getIdGenerator().newId();
    this.mqContext = config.getHazelcastMQInstance().createContext(transacted);

    mqContext.setAutoStart(false);
  }

  /**
   * Returns the unique ID of this session.
   *
   * @return the ID of this session
   */
  public String getId() {
    return id;
  }

  @Override
  public void close() throws JMSException {
    stop();

    mqContext.close();

    connection.onSessionClosed(this);
  }

  @Override
  public void commit() throws JMSException {
    mqContext.commit();
  }

  @Override
  public QueueBrowser createBrowser(Queue arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueueBrowser createBrowser(Queue arg0, String arg1)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesMessage createBytesMessage() throws JMSException {
    return new HazelcastMQJmsBytesMessage();
  }

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

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String arg1)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String arg1,
      String arg2, boolean arg3) throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public MapMessage createMapMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Message createMessage() throws JMSException {
    return createTextMessage();
  }

  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ObjectMessage createObjectMessage(Serializable arg0)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public MessageProducer createProducer(Destination destination)
      throws JMSException {
    HazelcastMQProducer mqProducer = mqContext.createProducer();

    HazelcastMQJmsMessageProducer producer = new HazelcastMQJmsMessageProducer(
        mqProducer, this, destination);
    return producer;
  }

  @Override
  public Queue createQueue(String queueName) throws JMSException {
    return new HazelcastMQJmsQueue(queueName);
  }

  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    return connection.createTemporaryQueue();
  }

  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    return connection.createTemporaryTopic();
  }

  @Override
  public TextMessage createTextMessage() throws JMSException {
    return new HazelcastMQJmsTextMessage();
  }

  @Override
  public TextMessage createTextMessage(String text) throws JMSException {
    TextMessage msg = createTextMessage();
    msg.setText(text);
    return msg;
  }

  @Override
  public Topic createTopic(String topicName) throws JMSException {
    return new HazelcastMQJmsTopic(topicName);
  }

  @Override
  public int getAcknowledgeMode() throws JMSException {
    if (getTransacted()) {
      return SESSION_TRANSACTED;
    }
    else {
      return AUTO_ACKNOWLEDGE;
    }
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getTransacted() throws JMSException {
    return mqContext.isTransacted();
  }

  @Override
  public void recover() throws JMSException {
    // no op
  }

  @Override
  public void rollback() throws JMSException {
    mqContext.rollback();
  }

  @Override
  public void run() {
    // no op
  }

  @Override
  public void setMessageListener(MessageListener arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsubscribe(String arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the JMS configuration for this session.
   *
   * @return the configuration for this session
   */
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
