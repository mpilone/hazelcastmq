package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of producing to a queue and attempting to consume from a different
 * node in a transaction.
 * 
 * @author mpilone
 */
public class TransactionalReceive extends ExampleApp {

  private final static ILogger log = Logger.
      getLogger(TransactionalReceive.class);

  private final String destination = "/queue/transactional.send.example";

  public static void main(String[] args) {
    TransactionalReceive app = new TransactionalReceive();
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
      HazelcastMQContext context1 = node1.createContext(false);
      HazelcastMQProducer producer = context1.createProducer();
      HazelcastMQConsumer consumer1 = context1.createConsumer(destination);

      HazelcastMQContext context2 = node2.createContext(true);
      HazelcastMQConsumer consumer2 = context2.createConsumer(destination);

      producer.send(destination, "Hello World!".getBytes());

      // Receive the message in the transacted consumer.
      HazelcastMQMessage msg = consumer2.receive(1, TimeUnit.SECONDS);
      Assert.notNull(msg, "Did not receive expected message.");
      log.info("Got message in transacted consumer but not committing yet.");

      // Try to receive the same message in the non-transacted consumer before
      // the first commits.
      msg = consumer1.receive(1, TimeUnit.SECONDS);
      Assert.isNull(msg, "Received a message that was already "
          + "consumed by another client.");

      // Roll back the transaction.
      log.info("Rolling back consumer transaction.");
      context2.rollback();

      // Try to receive the message now that it has been rolled back into the
      // queue.
      msg = consumer1.receive(1, TimeUnit.SECONDS);
      Assert.notNull(msg, "Did not receive expected message after rollback.");
      log.info("Got message body: " + new String(msg.getBody()));

      consumer1.close();
      context1.close();

      consumer2.close();
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
