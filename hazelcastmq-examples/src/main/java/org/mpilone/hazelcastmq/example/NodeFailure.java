package org.mpilone.hazelcastmq.example;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.mpilone.hazelcastmq.HazelcastMQConfig;
import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of producing to a queue and then having a node fail. There should be
 * no loss of messages.
 * 
 * @author mpilone
 */
public class NodeFailure {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private int msgCounter = 0;
  private String queueName = "node.failure.test";

  public static void main(String[] args) throws JMSException,
      InterruptedException {
    new NodeFailure();
  }

  public NodeFailure() throws JMSException, InterruptedException {
    // Create a three node cluster on localhost. We configure 2 backups so in
    // theory every node should have a complete copy of the queue we're using.
    Config config = new Config();
    NetworkConfig networkConfig = config.getNetworkConfig();
    networkConfig.setPort(10571);
    networkConfig.getInterfaces().addInterface("127.0.0.1");
    Join joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);
    joinConfig.getTcpIpConfig().setEnabled(true);
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10572");
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10573");
    config.getMapConfig("*").setBackupCount(2);
    ClusterNode node1 = new ClusterNode(config);

    config = new Config();
    networkConfig = config.getNetworkConfig();
    networkConfig.setPort(10572);
    networkConfig.getInterfaces().addInterface("127.0.0.1");
    joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);
    joinConfig.getTcpIpConfig().setEnabled(true);
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10571");
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10573");
    config.getMapConfig("*").setBackupCount(2);
    ClusterNode node2 = new ClusterNode(config);

    config = new Config();
    networkConfig = config.getNetworkConfig();
    networkConfig.setPort(10573);
    networkConfig.getInterfaces().addInterface("127.0.0.1");
    joinConfig = networkConfig.getJoin();
    joinConfig.getMulticastConfig().setEnabled(false);
    joinConfig.getTcpIpConfig().setEnabled(true);
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10571");
    joinConfig.getTcpIpConfig().addMember("127.0.0.1:10572");
    config.getMapConfig("*").setBackupCount(2);
    ClusterNode node3 = new ClusterNode(config);

    try {
      // Send and receive a message across nodes.
      log.info("\n\n*** Example 1");
      sendAndReceiveOnSingleOtherNode(node1, node3);

      // Queue up a couple messages and consume from two different nodes.
      log.info("\n\n*** Example 2");
      sendAndReceiveOnMultipleNodes(node1, node2, node3);

      // Queue up a couple of messages, kill the producing node, and consume on
      // another node.
      log.info("\n\n*** Example 3");
      sendKillAndReceiveOnMultipleNodes(node1, node2, node3);

      // Restart node 1 because the last example killed it.
      node1.restart();

      // Queue up a couple of messages, kill two of the nodes, and consume on
      // the remaining node.
      log.info("\n\n*** Example 4");
      sendKillTwoAndReceive(node1, node2, node3);

      // Restart nodes 1 and 2 because the last example killed it.
      node1.restart();
      node2.restart();

      // Queue up a couple of messages, kill two of the nodes, bring one back,
      // kill the other, and consume on the remaining node.
      log.info("\n\n*** Example 5");
      sendKillTwoRestartOneKillOneAndReceive(node1, node2, node3);
    }
    finally {
      node1.shutdown();
      node2.shutdown();
      node3.shutdown();
    }
  }

  private void sendKillTwoRestartOneKillOneAndReceive(ClusterNode node1,
      ClusterNode node2, ClusterNode node3) throws InterruptedException {
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);

    // Kill the first two nodes. Again, this may not prove too much because we
    // don't know where the original data landed in the cluster. There's a
    // chance the "master" data isn't sitting on node1 or node2 anyway.
    node1.kill();
    node2.kill();

    String msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);

    // Now restart node 2 and give it some time to join the cluster and migrate
    // data.
    node2.restart();
    Thread.sleep(10000);

    // Now kill node 3. In theory the remaining queued message should have
    // migrated to node 2.
    node3.kill();

    msg = node2.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 2: " + msg);
  }

  private void sendKillTwoAndReceive(ClusterNode node1, ClusterNode node2,
      ClusterNode node3) {
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);

    // Kill the first two nodes. Again, this may not prove too much because we
    // don't know where the original data landed in the cluster. There's a
    // chance the "master" data isn't sitting on node1 or node2 anyway.
    node1.kill();
    node2.kill();

    String msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);

    msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);

  }

  private void sendAndReceiveOnMultipleNodes(ClusterNode node1,
      ClusterNode node2, ClusterNode node3) {
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);

    String msg = node2.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 2: " + msg);

    msg = node1.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 1: " + msg);

    msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);

    msg = node2.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 2: " + msg);
  }

  private void sendAndReceiveOnSingleOtherNode(ClusterNode node1,
      ClusterNode node3) {
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);

    String msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);
  }

  private void sendKillAndReceiveOnMultipleNodes(ClusterNode node1,
      ClusterNode node2, ClusterNode node3) {
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);
    node1.getJmsOps().convertAndSend(queueName, "Hello " + msgCounter++);

    // Kill the node. This doesn't prove too much because we don't know where
    // the original data landed in the cluster. There's a good chance the
    // "master" data isn't sitting on node1 anyway.
    node1.kill();

    String msg = node2.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 2: " + msg);

    msg = node3.getJmsOps().receiveAndConvert(queueName).toString();
    log.info("Got message on node 3: " + msg);
  }

  /**
   * A cluster node which runs an instance of Hazelcast using the given
   * configuration.
   * 
   * @author mpilone
   */
  private static class ClusterNode {

    private HazelcastInstance hazelcast;
    private ConnectionFactory connectionFactory;
    private JmsTemplate jmsOps;
    private Config config;

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

    public void restart() {
      if (hazelcast != null) {
        shutdown();
      }

      hazelcast = Hazelcast.newHazelcastInstance(config);
      connectionFactory = new HazelcastMQConnectionFactory(hazelcast,
          new HazelcastMQConfig());
      jmsOps = new JmsTemplate(connectionFactory);
      jmsOps.setReceiveTimeout(2000);
    }

    public JmsTemplate getJmsOps() {
      return jmsOps;
    }

    public void kill() {
      if (hazelcast != null) {
        hazelcast.getLifecycleService().kill();
        hazelcast = null;
      }
    }

    public void shutdown() {
      if (hazelcast != null) {
        hazelcast.getLifecycleService().shutdown();
        hazelcast = null;
      }
    }
  }

}
