
package org.mpilone.hazelcastmq.example.spring.tx.support.hzmq;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.spring.tx.support.NoopTransactionManager;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastMQInstanceProxyFactory;
import org.springframework.context.annotation.*;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * Spring configuration for the
 * {@link TransactionAwareHazelcastMQInstanceProxyFactory} example.
 *
 * @author mpilone
 */
@Configuration
@EnableTransactionManagement
public class TransactionAwareConfig {

  @Bean(destroyMethod = "shutdown")
  public HazelcastInstance hazelcast() {
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    return Hazelcast.newHazelcastInstance(config);
  }

  @Bean(destroyMethod = "shutdown")
  public HazelcastMQInstance hazelcastMQ() {
    HazelcastMQConfig mqConfig = new HazelcastMQConfig(hazelcast());
    return HazelcastMQ.newHazelcastMQInstance(mqConfig);
  }

  @Bean
  public HazelcastMQInstance transactionAwareHazelcastMQ() {
    TransactionAwareHazelcastMQInstanceProxyFactory factory =
        new TransactionAwareHazelcastMQInstanceProxyFactory(hazelcastMQ(), true);

    return factory.create();
  }

  @Bean
  public NoopTransactionManager transactionManager() {
    return new NoopTransactionManager();
  }

  @Bean
  public BusinessService businessService() {
    return new BusinessService(transactionAwareHazelcastMQ());
  }

  @Bean(destroyMethod = "shutdown")
  public DemoQueueReader queueReader() {
    return new DemoQueueReader(transactionAwareHazelcastMQ());
  }

}
