package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
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

  private String queueName = "/queue/transactional.send.example";

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
    JoinConfig joinConfig = networkConfig.getJoin();
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
      HazelcastMQContext session1 = node1.createContext(true);

      HazelcastMQProducer producer = session1.createProducer();
      producer.send(queueName, "Hello World!".getBytes());

      HazelcastMQContext session2 = node2.createContext(false);

      HazelcastMQConsumer consumer = session2.createConsumer(queueName);

      // Try to consume the message that was sent in another node in a
      // transaction. This node shouldn't be able to see the message.
      HazelcastMQMessage msg = consumer.receive(1, TimeUnit.SECONDS);
      if (msg == null) {
        log.info("Did not get message (expected behavior).");
      }
      else {
        Assert.fail("Something went wrong. A transactional "
            + "message shouldn't be visible.");
      }

      // Commit the transaction so all nodes can see the transactional messages.
      log.info("Committing producer transaction.");
      session1.commit();
      session1.close();

      // Try to consume the message again. This time is should be visible.
      msg = consumer.receive(1, TimeUnit.SECONDS);
      Assert.notNull(msg, "Something went wrong. A message should be visible.");

      log.info("Got message body: " + new String(msg.getBody()));

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
    private HazelcastMQInstance mqInstance;

    /**
     * Constructs the node which will immediately start Hazelcast instance.
     * 
     * @param config
     *          the node configuration
     * @throws JMSException
     */
    public ClusterNode(Config config) {

      this.config = config;

      restart();
    }

    public void restart() {
      if (hazelcast != null) {
        shutdown();
      }

      // Hazelcast Instance
      hazelcast = Hazelcast.newHazelcastInstance(config);

      // HazelcastMQ Instance
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);

      mqInstance = HazelcastMQ.newHazelcastMQInstance(mqConfig);
    }

    public HazelcastMQContext createContext(boolean transacted) {
      HazelcastMQContext mqContext = mqInstance.createContext(transacted);
      mqContext.start();
      return mqContext;
    }

    public void shutdown() {
      if (hazelcast != null) {
        mqInstance.shutdown();

        hazelcast.getLifecycleService().shutdown();
        hazelcast = null;
      }
    }
  }

}
