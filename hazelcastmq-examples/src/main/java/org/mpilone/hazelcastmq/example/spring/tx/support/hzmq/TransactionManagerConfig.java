package org.mpilone.hazelcastmq.example.spring.tx.support.hzmq;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.spring.tx.HazelcastMQTransactionManager;
import org.springframework.context.annotation.*;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * Spring configuration for the {@link HazelcastMQTransactionManager} example.
 *
 * @author mpilone
 */
@Configuration
@EnableTransactionManagement
public class TransactionManagerConfig {

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
  public HazelcastMQTransactionManager transactionManager() {
    return new HazelcastMQTransactionManager(hazelcastMQ());
  }

  @Bean
  public BusinessService businessService() {
    return new BusinessServiceUsingUtils(hazelcastMQ());
  }

  @Bean(destroyMethod = "shutdown")
  public DemoQueueReader queueReader() {
    return new DemoQueueReader(hazelcastMQ());
  }

}
