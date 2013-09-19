package org.mpilone.hazelcastmq.jms;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQContext;
import org.mpilone.hazelcastmq.core.Headers;

/**
 * A JMS connection to a Hazelcast instance.
 * 
 * @author mpilone
 */
class HazelcastMQJmsConnection implements Connection {

  /**
   * The owning connection factory.
   */
  private HazelcastMQJmsConnectionFactory connectionFactory;

  private HazelcastMQJmsConfig config;

  /**
   * The client ID.
   */
  private String clientID;

  /**
   * The flag which indicates if the connection factory has been started or not.
   */
  private boolean active = false;

  /**
   * The list of all open sessions created by this connection.
   */
  private Map<String, HazelcastMQJmsSession> sessionMap;

  /**
   * The list of all open temporary destinations created by sessions of this
   * connection.
   */
  private Map<String, Destination> temporaryDestinationMap;

  private HazelcastMQContext mqContext;

  /**
   * Constructs the connection which is a child of the given connection factory.
   * 
   * @param connectionFactory
   *          the parent connection factory
   */
  public HazelcastMQJmsConnection(
      HazelcastMQJmsConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
    this.config = connectionFactory.getConfig();

    this.sessionMap = new ConcurrentHashMap<String, HazelcastMQJmsSession>();
    this.temporaryDestinationMap = new ConcurrentHashMap<String, Destination>();

    this.mqContext = config.getHazelcastMQInstance().createContext();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#close()
   */
  @Override
  public void close() throws JMSException {
    stop();

    for (HazelcastMQJmsSession session : sessionMap.values()) {
      session.close();
    }
    sessionMap.clear();

    mqContext.close();
    mqContext = null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#createConnectionConsumer(javax.jms.Destination,
   * java.lang.String, javax.jms.ServerSessionPool, int)
   */
  @Override
  public ConnectionConsumer createConnectionConsumer(Destination arg0,
      String arg1, ServerSessionPool arg2, int arg3) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic,
   * java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
   */
  @Override
  public ConnectionConsumer createDurableConnectionConsumer(Topic arg0,
      String arg1, String arg2, ServerSessionPool arg3, int arg4)
      throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#createSession(boolean, int)
   */
  @Override
  public Session createSession(boolean transacted, int acknowledgeMode)
      throws JMSException {
    // acknowledgeMode isn't supported.

    HazelcastMQJmsSession session = new HazelcastMQJmsSession(this, transacted);
    sessionMap.put(session.getId(), session);

    if (active) {
      session.start();
    }

    return session;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#getClientID()
   */
  @Override
  public String getClientID() throws JMSException {
    return clientID;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#getExceptionListener()
   */
  @Override
  public ExceptionListener getExceptionListener() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#getMetaData()
   */
  @Override
  public ConnectionMetaData getMetaData() throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#setClientID(java.lang.String)
   */
  @Override
  public void setClientID(String clientId) throws JMSException {
    this.clientID = clientId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
   */
  @Override
  public void setExceptionListener(ExceptionListener arg0) throws JMSException {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#start()
   */
  @Override
  public void start() throws JMSException {
    if (active) {
      return;
    }

    active = true;

    mqContext.start();

    for (HazelcastMQJmsSession session : sessionMap.values()) {
      session.start();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#stop()
   */
  @Override
  public void stop() throws JMSException {
    if (!active) {
      return;
    }

    active = false;

    for (HazelcastMQJmsSession session : sessionMap.values()) {
      session.stop();
    }

    mqContext.stop();
  }

  HazelcastMQJmsConfig getConfig() {
    return config;
  }

  /**
   * Indicates that the given session has been closed and no longer needs to be
   * tracked.
   * 
   * @param session
   *          the session that was closed
   */
  void onSessionClosed(HazelcastMQJmsSession session) {
    sessionMap.remove(session.getId());
  }

  TemporaryQueue createTemporaryQueue() {
    String destinationName = mqContext.createTemporaryQueue();

    String queueName = destinationName
        .substring(Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX.length());
    HazelcastMQJmsTemporaryQueue queue = new HazelcastMQJmsTemporaryQueue(this,
        queueName);

    temporaryDestinationMap.put(queue.getMqName(), queue);

    return queue;
  }

  TemporaryTopic createTemporaryTopic() {
    String destinationName = mqContext.createTemporaryTopic();

    String topicName = destinationName
        .substring(Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX.length());
    HazelcastMQJmsTemporaryTopic topic = new HazelcastMQJmsTemporaryTopic(this,
        topicName);

    temporaryDestinationMap.put(topic.getMqName(), topic);

    return topic;
  }

  // /**
  // * Adds the given destination to the list of temporary destinations to be
  // * cleaned up when this connection closes if not deleted earlier.
  // *
  // * @param dest
  // * the destination to add/track
  // */
  // void addTemporaryDestination(Destination dest) {
  // temporaryDestinations.add(dest);
  // }

  /**
   * Deletes the given temporary destination (either queue or topic).
   * 
   * @param dest
   *          the destination to delete
   * @throws JMSException
   */
  void deleteTemporaryDestination(Destination dest) throws JMSException {
    HazelcastMQJmsDestination mqDest = (HazelcastMQJmsDestination) dest;

    if (temporaryDestinationMap.remove(mqDest.getMqName()) != null) {
      mqContext.destroyTemporaryDestination(mqDest.getMqName());
    }
  }

}
