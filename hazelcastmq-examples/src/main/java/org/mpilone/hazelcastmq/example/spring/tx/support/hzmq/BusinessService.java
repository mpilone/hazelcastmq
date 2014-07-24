package org.mpilone.hazelcastmq.example.spring.tx.support.hzmq;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;
import org.springframework.transaction.annotation.Transactional;

/**
 * A simple business service to be used in the transaction examples.
 *
 * @author mpilone
 */
public class BusinessService {

  private HazelcastMQInstance hazelcastMQInstance;
  private final static Logger log = LoggerFactory.getLogger(
      BusinessService.class);

  public BusinessService() {
    this.hazelcastMQInstance = null;
  }

  public BusinessService(HazelcastMQInstance hazelcastInstance) {
    this();

    this.hazelcastMQInstance = hazelcastInstance;
  }

  public void setHazelcastMQInstance(HazelcastMQInstance hazelcastInstance) {
    this.hazelcastMQInstance = hazelcastInstance;
  }

  /**
   * Sleeps for the given duration.
   *
   * @param duration the amount of time to sleep
   * @param unit the unit of the duration
   */
  protected void sleep(long duration, TimeUnit unit) {
    try {
      Thread.sleep(unit.toMillis(duration));
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Transactional
  public void processWithTransaction() {

    try (HazelcastMQContext mqContext = getContext(hazelcastMQInstance)) {

      HazelcastMQProducer producer = mqContext.createProducer(
          "/queue/demo.queue");

      log.info("Sending to queue in transaction.");
      producer.send(getClass().getName() + ": processWithTransaction");

      log.info("Sleeping.");

      sleep(5, TimeUnit.SECONDS);

      log.info("Done.");
    }
  }

  public void processWithoutTransaction() {

    try (HazelcastMQContext mqContext = getContext(hazelcastMQInstance)) {

      HazelcastMQProducer producer = mqContext.createProducer(
          "/queue/demo.queue");

      log.info("Sending to queue outside transaction.");
      producer.send(getClass().getName() + ": processWithoutTransaction");

      log.info("Sleeping.");

      sleep(5, TimeUnit.SECONDS);

      log.info("Done.");
    }
  }

  @Transactional
  public void processWithTransactionAndException() {
    try (HazelcastMQContext mqContext = getContext(hazelcastMQInstance)) {

      HazelcastMQProducer producer = mqContext.createProducer(
          "/queue/demo.queue");

      log.info("Sending to queue in transaction.");
      producer.send(getClass().getName()
          + ": processWithTransactionAndException");

      log.info("Sleeping.");

      sleep(5, TimeUnit.SECONDS);

      log.info("Throwing exception in transaction.");
      throw new RuntimeException("Better roll back.");
    }
  }

  /**
   * Gets a context from the MQ instance. The default implementation simply uses
   * {@link HazelcastMQInstance#createContext()} but subclasses can use
   * different strategies.
   *
   * @param hazelcastMQInstance the HazelcastMQInstance to get the context from
   *
   * @return the context instance
   */
  protected HazelcastMQContext getContext(
      HazelcastMQInstance hazelcastMQInstance) {
    return hazelcastMQInstance.createContext();
  }

}
