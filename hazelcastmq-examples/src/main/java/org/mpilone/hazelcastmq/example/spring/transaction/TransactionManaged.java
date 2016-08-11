
package org.mpilone.hazelcastmq.example.spring.transaction;

import org.mpilone.hazelcastmq.core.Broker;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.example.spring.transaction.support.BusinessService;
import org.mpilone.hazelcastmq.example.spring.transaction.support.TransactionManagedConfig;
import org.springframework.context.annotation.*;

import com.hazelcast.spring.transaction.ManagedTransactionalTaskContext;



/**
 * An example of using the {@link ManagedTransactionalTaskContext} to
 * synchronize the {@link Broker} transaction with an Hazelcast transaction on a
 * Hazelcast transaction manager.
 *
 * @author mpilone
 */
public class TransactionManaged extends ExampleApp {

  public static void main(String[] args) {
    TransactionManaged app = new TransactionManaged();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionManagedConfig.class)) {

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
