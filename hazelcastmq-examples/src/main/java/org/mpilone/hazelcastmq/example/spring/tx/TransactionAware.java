package org.mpilone.hazelcastmq.example.spring.tx;

import org.mpilone.hazelcastmq.example.spring.tx.support.*;
import org.mpilone.hazelcastmq.spring.tx.TransactionAwareHazelcastInstanceProxyFactory;
import org.springframework.context.annotation.*;

import com.hazelcast.core.HazelcastInstance;

/**
 * An example of using the {@link TransactionAwareHazelcastInstanceProxyFactory}
 * to create a proxied {@link HazelcastInstance} that will automatically
 * participate in Spring managed transactions.
 *
 * @author mpilone
 */
public class TransactionAware {

  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "warn");

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
