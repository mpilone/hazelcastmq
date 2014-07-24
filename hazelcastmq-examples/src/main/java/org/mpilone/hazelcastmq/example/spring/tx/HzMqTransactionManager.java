
package org.mpilone.hazelcastmq.example.spring.tx;

import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.example.spring.tx.support.hzmq.BusinessService;
import org.mpilone.hazelcastmq.example.spring.tx.support.hzmq.TransactionManagerConfig;
import org.mpilone.hazelcastmq.spring.tx.HazelcastMQTransactionManager;
import org.springframework.context.annotation.*;



/**
 * An example of using the {@link HazelcastMQTransactionManager} to manage the
 * transactions of a single {@link HazelcastMQInstance} by integrating into the
 * Spring transaction management framework.
 *
 * @author mpilone
 */
public class HzMqTransactionManager extends ExampleApp {

  public static void main(String[] args) {
    HzMqTransactionManager app = new HzMqTransactionManager();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionManagerConfig.class)) {

      // Run the business methods.
      BusinessService businessService =
          (BusinessService) springContext.getBean("businessService");
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
