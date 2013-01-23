package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import java.io.Serializable;

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

/**
 * A JMS session in HazelcastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQSession implements Session {

  /**
   * The parent connection.
   */
  private HazelcastMQConnection connection;

  /**
   * The Hazelcast instance that the session is working with.
   */
  private HazelcastInstance hazelcast;

  /**
   * The flag which indicates if the session is transacted.
   */
  private boolean transacted;

  /**
   * The ID generator used to generate unique IDs for temporary queues and
   * topics.
   */
  private IdGenerator idGenerator;

  /**
   * The log for this class.
   */
  @SuppressWarnings("unused")
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The prefix used when creating new temporary queues, temporary topics, and
   * ID generators.
   */
  private static final String KEY_PREFIX = "hazelcastMQ";

  /**
   * Constructs the session.
   * 
   * @param connection
   *          the parent connection
   * @param transacted
   *          true for transactional sending, false otherwise
   */
  public HazelcastMQSession(HazelcastMQConnection connection, boolean transacted) {
    this.connection = connection;
    this.transacted = transacted;

    this.hazelcast = this.connection.getHazelcast();

    if (transacted) {
      hazelcast.getTransaction().begin();
    }

    idGenerator = hazelcast.getIdGenerator(KEY_PREFIX + ".idgenerator");
  }

  /**
   * Returns the ID generator for this session.
   * 
   * @return the ID generator
   */
  public IdGenerator getIdGenerator() {
    return idGenerator;
  }

  /**
   * Returns the Hazelcast instance for this session.
   * 
   * @return the Hazelcast instance
   */
  public HazelcastInstance getHazelcast() {
    return hazelcast;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#close()
   */
  @Override
  public void close() throws JMSException {
    if (transacted) {
      hazelcast.getTransaction().rollback();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#commit()
   */
  @Override
  public void commit() throws JMSException {
    if (transacted) {
      hazelcast.getTransaction().commit();
      hazelcast.getTransaction().begin();
    }
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
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createConsumer(javax.jms.Destination)
   */
  @Override
  public MessageConsumer createConsumer(Destination destination)
      throws JMSException {
    if (destination instanceof HazelcastMQTopic) {
      return new HazelcastMQTopicSubscriber(this,
          (HazelcastMQTopic) destination);
    }
    else {
      return new HazelcastMQQueueReceiver(this, (HazelcastMQQueue) destination);
    }
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
    return new HazelcastMQMessage();
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
    return new HazelcastMQMessageProducer(this, destination);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createQueue(java.lang.String)
   */
  @Override
  public Queue createQueue(String queueName) throws JMSException {
    return new HazelcastMQQueue(this, queueName);
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

    return new HazelcastMQQueue(this, KEY_PREFIX + ".tmp.queue."
        + idGenerator.newId());
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTemporaryTopic()
   */
  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {

    return new HazelcastMQTopic(this, KEY_PREFIX + ".tmp.queue."
        + idGenerator.newId());
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTextMessage()
   */
  @Override
  public TextMessage createTextMessage() throws JMSException {
    return new HazelcastMQTextMessage();
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
    return new HazelcastMQTopic(this, topicName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#getAcknowledgeMode()
   */
  @Override
  public int getAcknowledgeMode() throws JMSException {
    if (transacted) {
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
    return transacted;
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
    if (transacted) {
      hazelcast.getTransaction().rollback();
      hazelcast.getTransaction().begin();
    }
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

}
