
package org.mpilone.hazelcastmq.example.spring.tx;

import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.example.spring.tx.support.hz.BusinessService;
import org.mpilone.hazelcastmq.example.spring.tx.support.hz.TransactionManagerConfig;
import org.mpilone.hazelcastmq.spring.tx.HazelcastTransactionManager;


import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


/**
 * An example of using the {@link HazelcastTransactionManager} to manage the
 * transactions of a single {@link HazelcastInstance} by integrating into the
 * Spring transaction management framework.
 *
 * @author mpilone
 */
public class HzTransactionManager extends ExampleApp {

  public static void main(String[] args) {
    HzTransactionManager app = new HzTransactionManager();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionManagerConfig.class)) {

      // Run the business methods.
      BusinessService businessService = (BusinessService) springContext.getBean("businessService");
      businessService.processWithTransaction();
      businessService.processWithoutTransaction();
      try {
        businessService.processWithTransactionAndException();
      }
      catch (RuntimeException ex) {
        // expected.
      }
    }
  }

}
