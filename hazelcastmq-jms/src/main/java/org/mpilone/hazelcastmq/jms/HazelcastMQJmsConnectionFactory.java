package org.mpilone.hazelcastmq.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * A JMS connection factory which can create connections to a Hazelcast
 * instance.
 * 
 * @author mpilone
 */
public class HazelcastMQJmsConnectionFactory implements ConnectionFactory {

  /**
   * The MQ JMS layer configuration.
   */
  private HazelcastMQJmsConfig config;

  /**
   * Constructs the factory with no Hazelcast instance and a default
   * configuration. The Hazelcast instance must be set before use.
   */
  public HazelcastMQJmsConnectionFactory() {
    this(new HazelcastMQJmsConfig());
  }

  /**
   * Constructs the factory with the given Hazelcast instance and configuration.
   * 
   * @param config  the configuration of the MQ layer
   */
  public HazelcastMQJmsConnectionFactory(HazelcastMQJmsConfig config) {
    super();
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
    return new HazelcastMQJmsConnection(this);
  }

  /**
   * Returns the MQ layer configuration.
   * 
   * @return the MQ configuration
   */
  public HazelcastMQJmsConfig getConfig() {
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
  public void setConfig(HazelcastMQJmsConfig config) {
    this.config = config;
  }
}
