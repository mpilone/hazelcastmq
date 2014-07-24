package org.mpilone.hazelcastmq.example.spring.tx;

import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.example.spring.tx.support.hzmq.BusinessService;
import org.mpilone.hazelcastmq.example.spring.tx.support.hzmq.TransactionAwareConfig;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastMQInstanceProxyFactory;
import org.springframework.context.annotation.*;


/**
 * An example of using the
 * {@link TransactionAwareHazelcastMQInstanceProxyFactory} to create a proxied
 * {@link HazelcastMQInstance} that will automatically participate in Spring
 * managed transactions.
 *
 * @author mpilone
 */
public class HzMqTransactionAware extends ExampleApp {

  public static void main(String[] args) {
    HzMqTransactionAware app = new HzMqTransactionAware();
    app.runExample();
  }

  @Override
  protected void start() {

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionAwareConfig.class)) {

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
