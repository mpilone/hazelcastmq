package org.mpilone.hazelcastmq;

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
   * Constructs the connection which is a child of the given connection factory.
   * 
   * @param connectionFactory
   *          the parent connection factory
   */
  public HazelcastMQConnection(HazelcastMQConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#close()
   */
  @Override
  public void close() throws JMSException {
    // no op
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

    return new HazelcastMQSession(this, transacted);
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
    // no op
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Connection#stop()
   */
  @Override
  public void stop() throws JMSException {
    // no op
  }

  public HazelcastInstance getHazelcast() {
    return connectionFactory.getHazelcast();
  }

}
