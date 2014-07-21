
package org.mpilone.hazelcastmq.example.spring.tx.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

/**
 * A transaction manager that doesn't manage any direct resource. The manager is
 * used to test Hazelcast transaction synchronizations to an arbitrary
 * transaction manager.
 *
 * @author mpilone
 */
public class NoopTransactionManager extends AbstractPlatformTransactionManager {

  private final static Logger log = LoggerFactory.getLogger(
      NoopTransactionManager.class);

  @Override
  protected Object doGetTransaction() throws TransactionException {
    log.info("In NoopTransactionManager::doGetTransaction");

    return new Object();
  }

  @Override
  protected void doBegin(Object o, TransactionDefinition td) throws
      TransactionException {
    log.info("In NoopTransactionManager::doBegin");
  }

  @Override
  protected void doCommit(DefaultTransactionStatus dts) throws
      TransactionException {
    log.info("In NoopTransactionManager::doCommit");
  }

  @Override
  protected void doRollback(DefaultTransactionStatus dts) throws
      TransactionException {
    log.info("In NoopTransactionManager::doRollback");
  }

}
