
package org.mpilone.hazelcastmq.example.spring.tx;

import org.mpilone.hazelcastmq.example.spring.tx.support.BusinessService;
import org.mpilone.hazelcastmq.example.spring.tx.support.TransactionManagerConfig;
import org.mpilone.hazelcastmq.spring.tx.HazelcastTransactionManager;
import org.springframework.context.annotation.*;

import com.hazelcast.core.HazelcastInstance;


/**
 * An example of using the {@link HazelcastTransactionManager} to manage the
 * transactions of a single {@link HazelcastInstance} by integrating into the
 * Spring transaction management framework.
 *
 * @author mpilone
 */
public class TransactionManager {

  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    // Run the business methods.
    try (AnnotationConfigApplicationContext springContext =
        new AnnotationConfigApplicationContext(TransactionManagerConfig.class)) {

      // Run the business methods.
      BusinessService businessService =
          (BusinessService) springContext.getBean(
              "businessService");
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
