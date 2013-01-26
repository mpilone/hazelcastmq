package org.mpilone.hazelcastmq.example;

import javax.jms.*;

import org.mpilone.hazelcastmq.HazelcastMQConfig;
import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of producing to a queue within a transaction and attempting to
 * consume from a different node.
 * 
 * @author mpilone
 */
public class TransactionalSend {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private String queueName = "transactional.send.example";

  public static void main(String[] args) throws JMSException,
      InterruptedException {
    new TransactionalSend();
  }

  public TransactionalSend() throws JMSException, InterruptedException {
    // Create a two node cluster on localhost. We need to use two separate nodes
    // (or two separate threads) because transactional messages produced can be
    // consumed in the same thread before the transaction is committed. This is
    // because all Hazelcast transactions are thread bound and don't know
    // anything about JMS sessions. Therefore two separate sessions in the same
    // thread will be transactional if either one is transactional.
    Config config = new Config();
    NetworkConfig networkConfig = config.getNetworkConfig();
    networkConfig.setPort(10571);
    networkConfig.getInterfaces().addInterface("127.0.0.1");
    Join joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);
    joinConfig.getTcpIpConfig().setEnabled(true);
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10572");
    ClusterNode node1 = new ClusterNode(config);

    config = new Config();
    networkConfig = config.getNetworkConfig();
    networkConfig.setPort(10572);
    networkConfig.getInterfaces().addInterface("127.0.0.1");
    joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);
    joinConfig.getTcpIpConfig().setEnabled(true);
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10571");
    ClusterNode node2 = new ClusterNode(config);

    try {
      Session session1 = node1.getConnection().createSession(true,
          Session.AUTO_ACKNOWLEDGE);
      Destination destination1 = session1.createQueue(queueName);
      MessageProducer producer = session1.createProducer(destination1);

      Message msg = session1.createTextMessage("Hello World!");
      producer.send(msg);
      producer.close();

      Session session2 = node2.getConnection().createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Destination destination2 = session2.createQueue(queueName);
      MessageConsumer consumer = session2.createConsumer(destination2);

      // Try to consume the message that was sent in another node in a
      // transaction. This node shouldn't be able to see the message.
      msg = consumer.receive(1000);
      if (msg == null) {
        log.info("Did not get message (expected behavior).");
      }
      else {
        log.info("Something went wrong. A transactional message shouldn't be visible.");
      }

      // Commit the transaction so all nodes can see the transactional messages.
      session1.commit();
      session1.close();

      // Try to consume the message again. This time is should be visible.
      msg = consumer.receive(1000);
      if (msg == null) {
        log.info("Something went wrong. A message should be visible.");
      }
      else {
        log.info("Got message: " + ((TextMessage) msg).getText());
      }

      session2.close();
    }
    finally {
      node1.shutdown();
      node2.shutdown();
    }
  }

  /**
   * A cluster node which runs an instance of Hazelcast using the given
   * configuration.
   * 
   * @author mpilone
   */
  private static class ClusterNode {

    private HazelcastInstance hazelcast;
    private Config config;
    private ConnectionFactory connectionFactory;
    private Connection connection;

    /**
     * Constructs the node which will immediately start Hazelcast instance.
     * 
     * @param config
     *          the node configuration
     * @throws JMSException
     */
    public ClusterNode(Config config) throws JMSException {

      this.config = config;

      restart();
    }

    public void restart() throws JMSException {
      if (hazelcast != null) {
        shutdown();
      }

      hazelcast = Hazelcast.newHazelcastInstance(config);
      connectionFactory = new HazelcastMQConnectionFactory(hazelcast,
          new HazelcastMQConfig());
      connection = connectionFactory.createConnection();
      connection.start();
    }

    public Connection getConnection() {
      return connection;
    }

    public void shutdown() throws JMSException {
      if (hazelcast != null) {
        connection.close();

        hazelcast.getLifecycleService().shutdown();
        hazelcast = null;
      }
    }
  }

}
