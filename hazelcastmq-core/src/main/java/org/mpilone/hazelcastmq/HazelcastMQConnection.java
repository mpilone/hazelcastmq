package org.mpilone.hazelcastmq;

import java.util.ArrayList;
import java.util.List;

import javax.jms.*;

import com.hazelcast.core.HazelcastInstance;

/**
 * A JMS connection to a Hazelcast instance.
 * 
 * @author mpilone
 */
public class HazelcastMQConnection implements Connection {

  /**
   * The owning connection factory.
   */
  private HazelcastMQConnectionFactory connectionFactory;

  /**
   * The client ID.
   */
  private String clientID;

  /**
   * The flag which indicates if the connection factory has been started or not.
   */
  private boolean started = false;

  /**
   * The list of all open sessions created by this connection.
   */
  private List<HazelcastMQSession> sessions;

  /**
   * The list of all open temporary destinations created by sessions of this
   * connection.
   */
  private List<Destination> temporaryDestinations;

  /**
   * Constructs the connection which is a child of the given connection factory.
   * 
   * @param connectionFactory
   *          the parent connection factory
   */
  public HazelcastMQConnection(HazelcastMQConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;

    this.sessions = new ArrayList<HazelcastMQSession>();
    this.temporaryDestinations = new ArrayList<Destination>();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#close()
   */
  @Override
  public void close() throws JMSException {
    for (HazelcastMQSession session : sessions) {
      session.close();
    }

    List<Destination> tmpDests = new ArrayList<Destination>(
        temporaryDestinations);
    for (Destination destination : tmpDests) {
      deleteTemporaryDestination(destination);
    }
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

    HazelcastMQSession session = new HazelcastMQSession(this, transacted);
    sessions.add(session);

    if (started) {
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
    started = true;

    for (HazelcastMQSession session : sessions) {
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
    started = false;

    for (HazelcastMQSession session : sessions) {
      session.stop();
    }
  }

  /**
   * Returns the flag which indicates if the connection has been started or not.
   * 
   * @return the started flag
   */
  boolean isStarted() {
    return started;
  }

  HazelcastInstance getHazelcast() {
    return connectionFactory.getHazelcast();
  }

  HazelcastMQConfig getConfig() {
    return connectionFactory.getConfig();
  }

  /**
   * Indicates that the given session has been closed and no longer needs to be
   * tracked.
   * 
   * @param session
   *          the session that was closed
   */
  void sessionClosed(HazelcastMQSession session) {
    sessions.remove(session);
  }

  /**
   * Adds the given destination to the list of temporary destinations to be
   * cleaned up when this connection closes if not deleted earlier.
   * 
   * @param dest
   *          the destination to add/track
   */
  void addTemporaryDestination(Destination dest) {
    temporaryDestinations.add(dest);
  }

  /**
   * Deletes the given temporary destination (either queue or topic).
   * 
   * @param dest
   *          the destination to delete
   * @throws JMSException
   */
  void deleteTemporaryDestination(Destination dest) throws JMSException {
    if (temporaryDestinations.remove(dest)) {
      if (dest instanceof TemporaryQueue) {
        getHazelcast().getQueue(((TemporaryQueue) dest).getQueueName())
            .destroy();
      }
      else if (dest instanceof TemporaryTopic) {
        getHazelcast().getTopic(((TemporaryTopic) dest).getTopicName())
            .destroy();
      }

    }
  }

}
