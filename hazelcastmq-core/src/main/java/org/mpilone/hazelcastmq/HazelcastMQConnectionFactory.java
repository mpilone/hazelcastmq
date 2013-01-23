package org.mpilone.hazelcastmq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import com.hazelcast.core.HazelcastInstance;

/**
 * A JMS connection factory which can create connections to a Hazelcast
 * instance.
 * 
 * @author mpilone
 */
public class HazelcastMQConnectionFactory implements ConnectionFactory {

  /**
   * The Hazelcast instance to use for all created connections.
   */
  private HazelcastInstance hazelcast;

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.ConnectionFactory#createConnection()
   */
  @Override
  public Connection createConnection() throws JMSException {
    return createConnection(null, null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.ConnectionFactory#createConnection(java.lang.String,
   * java.lang.String)
   */
  @Override
  public Connection createConnection(String username, String password)
      throws JMSException {
    return new HazelcastMQConnection(this);
  }

  /**
   * Sets the Hazelcast instance to use for all created connections.
   * 
   * @param hazelcast
   *          the hazelcast instance
   */
  public void setHazelcast(HazelcastInstance hazelcast) {
    this.hazelcast = hazelcast;
  }

  /**
   * Returns the Hazelcast instance used for all connections.
   * 
   * @return the Hazelcast instance
   */
  public HazelcastInstance getHazelcast() {
    return hazelcast;
  }
}
