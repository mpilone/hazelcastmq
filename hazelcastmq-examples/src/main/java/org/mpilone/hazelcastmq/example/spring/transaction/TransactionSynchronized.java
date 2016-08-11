package org.mpilone.hazelcastmq.example.spring.transaction;

import org.mpilone.hazelcastmq.core.Broker;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.example.spring.transaction.support.BusinessService;
import org.mpilone.hazelcastmq.example.spring.transaction.support.TransactionSynchronizedConfig;
import org.mpilone.hazelcastmq.spring.transaction.SynchronizedTransactionalTaskContext;
import org.springframework.context.annotation.*;


/**
 * An example of using the
 * {@link SynchronizedTransactionalTaskContext} to
 * synchronize the {@link Broker} transaction with an Hazelcast transaction on a
 * non-Hazelcast transaction manager.
 *
 * @author mpilone
 */
public class TransactionSynchronized extends ExampleApp {

  public static void main(String[] args) {
    TransactionSynchronized app = new TransactionSynchronized();
    app.runExample();
  }

  @Override
  protected void start() {

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionSynchronizedConfig.class)) {

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
