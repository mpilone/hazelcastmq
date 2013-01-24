package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
  private boolean transacted = false;

  /**
   * The flag which indicates if this session has been started yet.
   */
  private boolean started = false;

  /**
   * The list of all open consumers created by this session.
   */
  private List<HazelcastMQMessageConsumer> consumers;

  /**
   * The list of all open producers created by this session.
   */
  private List<HazelcastMQMessageProducer> producers;

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

    this.idGenerator = hazelcast.getIdGenerator(KEY_PREFIX + ".idgenerator");
    this.consumers = new ArrayList<HazelcastMQMessageConsumer>();
    this.producers = new ArrayList<HazelcastMQMessageProducer>();
  }

  /**
   * Returns the ID generator for this session.
   * 
   * @return the ID generator
   */
  IdGenerator getIdGenerator() {
    return idGenerator;
  }

  /**
   * Returns the Hazelcast instance for this session.
   * 
   * @return the Hazelcast instance
   */
  HazelcastInstance getHazelcast() {
    return hazelcast;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#close()
   */
  @Override
  public void close() throws JMSException {
    stop();

    for (MessageProducer producer : producers) {
      producer.close();
    }
    producers.clear();

    for (MessageConsumer consumer : consumers) {
      consumer.close();
    }
    consumers.clear();

    connection.sessionClosed(this);

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
    HazelcastMQMessageConsumer consumer = null;

    if (destination instanceof HazelcastMQTopic) {
      consumer = new HazelcastMQTopicSubscriber(this,
          (HazelcastMQTopic) destination);
    }
    else {
      consumer = new HazelcastMQQueueReceiver(this,
          (HazelcastMQQueue) destination);
    }

    if (started) {
      consumer.start();
    }

    consumers.add(consumer);
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
    HazelcastMQMessageProducer producer = new HazelcastMQMessageProducer(this,
        destination);
    producers.add(producer);
    return producer;
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

    TemporaryQueue queue = new HazelcastMQTemporaryQueue(this, KEY_PREFIX
        + ".tmp.queue." + idGenerator.newId());
    connection.addTemporaryDestination(queue);
    return queue;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Session#createTemporaryTopic()
   */
  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {

    TemporaryTopic topic = new HazelcastMQTemporaryTopic(this, KEY_PREFIX
        + ".tmp.queue." + idGenerator.newId());
    connection.addTemporaryDestination(topic);
    return topic;
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

  HazelcastMQConfig getConfig() {
    return connection.getConfig();
  }

  /**
   * Returns the parent connection of this session.
   * 
   * @return the parent connection
   */
  HazelcastMQConnection getConnection() {
    return connection;
  }

  /**
   * Indicates that the producer was closed and no longer needs to be tracked.
   * 
   * @param producer
   *          the producer that was closed
   */
  void producerClosed(MessageProducer producer) {
    producers.remove(producer);
  }

  /**
   * Indicates that the consumer was closed and no longer needs to be tracked.
   * 
   * @param consumer
   *          the consumer that was closed
   */
  void consumerClosed(MessageConsumer consumer) {
    consumers.remove(consumer);
  }

  /**
   * Starts this session. This method must be called by the connection when it
   * is started.
   */
  void start() {
    started = true;
    for (HazelcastMQMessageConsumer consumer : consumers) {
      consumer.start();
    }
  }

  /**
   * Stops this session. This method must be called by the connection when it is
   * stopped.
   */
  void stop() {
    started = false;
    for (HazelcastMQMessageConsumer consumer : consumers) {
      consumer.stop();
    }
  }

}
