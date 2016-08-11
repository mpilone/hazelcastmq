package org.mpilone.hazelcastmq.example.spring.transaction.support;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.spring.transaction.TransactionAwareBrokerProxy;
import org.springframework.context.annotation.*;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.spring.transaction.HazelcastTransactionManager;
import com.hazelcast.spring.transaction.ManagedTransactionalTaskContext;

/**
 * Spring configuration that uses the {@link HazelcastTransactionManager}
 * example.
 *
 * @author mpilone
 */
@Configuration
@EnableTransactionManagement
public class TransactionManagedConfig {

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

  @Bean(destroyMethod = "close")
  public Broker transactionAwareBroker() {
    // Synchronize the transactionAwareBroker transaction with the non-Hazelcast
    // transaction manager.
    ManagedTransactionalTaskContext txContext =
        new ManagedTransactionalTaskContext(transactionManager());

    // Create the proxy to the transactionAwareBroker.
    TransactionAwareBrokerProxy proxy = new TransactionAwareBrokerProxy(
        txContext, broker());

    return proxy;
  }

  @Bean
  public HazelcastTransactionManager transactionManager() {
    return new HazelcastTransactionManager(hazelcast());
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
