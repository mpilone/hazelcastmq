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

  /**
   * The MQ layer configuration.
   */
  private HazelcastMQConfig config;

  /**
   * Constructs the factory with no Hazelcast instance and a default
   * configuration. The Hazelcast instance must be set before use.
   */
  public HazelcastMQConnectionFactory() {
    this(null, new HazelcastMQConfig());
  }

  /**
   * Constructs the factory with the given Hazelcast instance and configuration.
   * 
   * @param hazelcast
   *          the Hazelcast instance to use for all message passing
   * @param config
   *          the configuration of the MQ layer
   */
  public HazelcastMQConnectionFactory(HazelcastInstance hazelcast,
      HazelcastMQConfig config) {
    super();
    this.hazelcast = hazelcast;
    this.config = config;
  }

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

  /**
   * Returns the MQ layer configuration.
   * 
   * @return the MQ configuration
   */
  public HazelcastMQConfig getConfig() {
    return config;
  }

  /**
   * Sets the MQ layer configuration which replaces any existing configuration.
   * The behavior is undefined if the configuration changes while there are
   * existing connections and session from this connection factory.
   * 
   * @param config
   *          the new MQ configuration
   */
  public void setConfig(HazelcastMQConfig config) {
    this.config = config;
  }
}
