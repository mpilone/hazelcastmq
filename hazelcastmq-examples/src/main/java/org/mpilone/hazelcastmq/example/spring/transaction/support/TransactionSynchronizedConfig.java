package org.mpilone.hazelcastmq.example.spring.transaction.support;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.spring.transaction.SynchronizedTransactionalTaskContext;
import org.mpilone.hazelcastmq.spring.transaction.TransactionAwareBrokerProxy;
import org.springframework.context.annotation.*;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * Spring configuration for the {@link SynchronizedTransactionalTaskContext}
 * example.
 *
 * @author mpilone
 */
@Configuration
@EnableTransactionManagement
public class TransactionSynchronizedConfig {

  @Bean(destroyMethod = "shutdown")
  public HazelcastInstance hazelcast() {
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    return Hazelcast.newHazelcastInstance(config);
  }

  @Bean(destroyMethod = "close")
  public Broker broker() {
    BrokerConfig mqConfig = new BrokerConfig(hazelcast());
    return HazelcastMQ.newBroker(mqConfig);
  }

  @Bean
  public Broker transactionAwareBroker() {

    // Synchronize the broker transaction with the non-Hazelcast
    // transaction manager.
    SynchronizedTransactionalTaskContext txContext =
        new SynchronizedTransactionalTaskContext(hazelcast());

    // Create the proxy to the broker.
    TransactionAwareBrokerProxy proxy = new TransactionAwareBrokerProxy(
        txContext, broker());

    return proxy;
  }

  @Bean
  public NoopTransactionManager transactionManager() {
    return new NoopTransactionManager();
  }

  @Bean
  public BusinessService businessService() {
    return new BusinessService(transactionAwareBroker());
  }

  @Bean(destroyMethod = "shutdown")
  public DemoQueueReader queueReader() {
    return new DemoQueueReader(transactionAwareBroker());
  }

}
