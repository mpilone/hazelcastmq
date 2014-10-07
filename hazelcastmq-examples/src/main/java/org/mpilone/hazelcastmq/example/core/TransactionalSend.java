package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.*;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of producing to a queue within a transaction and attempting to
 * consume from a different node.
 * 
 * @author mpilone
 */
public class TransactionalSend extends ExampleApp {

  private final ILogger log = Logger.getLogger(TransactionalSend.class);

  private final String queueName = "/queue/transactional.send.example";

  public static void main(String[] args)  {
    TransactionalSend app = new TransactionalSend();
    app.runExample();
  }

  @Override
  public void start() throws Exception {
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
      HazelcastMQContext context1 = node1.createContext(true);

      HazelcastMQProducer producer = context1.createProducer();
      
      HazelcastMQContext context2 = node2.createContext(false);
      HazelcastMQConsumer consumer = context2.createConsumer(queueName);

      // Send the first message while in a transaction.
      producer.send(queueName, "Hello World!".getBytes());

      // Try to consume the message that was sent in another node in a
      // transaction. This node shouldn't be able to see the message.
      HazelcastMQMessage msg = consumer.receive(1, TimeUnit.SECONDS);
      Assert.isNull(msg, "Something went wrong. A transactional message "
          + "shouldn't be visible.");

      // Commit the transaction so all nodes can see the transactional messages.
      log.info("Committing producer transaction 1.");
      context1.commit();

      // Try to consume the message again. This time is should be visible.
      msg = consumer.receive(1, TimeUnit.SECONDS);
      Assert.notNull(msg, "Something went wrong. A message should be visible.");
      log.info("Got message body: " + new String(msg.getBody()));

      // Send another message in another transaction.
      producer.send(queueName, "Goodbye World!".getBytes());
      log.info("Committing producer transaction 2.");
      context1.commit();

      msg = consumer.receive(1, TimeUnit.SECONDS);
      Assert.notNull(msg, "Something went wrong. A message should be visible.");
      log.info("Got message body: " + new String(msg.getBody()));

      context1.close();
      context2.close();
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
