package org.mpilone.hazelcastmq.stomper;

import javax.jms.Session;

/**
 * An active transaction for a client.
 * 
 * @author mpilone
 */
class ClientTransaction {
  /**
   * The unique ID of the transaction.
   */
  private String transactionId;

  /**
   * The transacted JMS session used for all message production within the
   * transaction.
   */
  private Session session;

  /**
   * Constructs the transaction.
   * 
   * @param transactionId
   *          the unique ID of the transaction
   * @param session
   *          the transacted JMS session used for all message production within
   *          the transaction
   */
  public ClientTransaction(String transactionId, Session session) {
    super();
    this.transactionId = transactionId;
    this.session = session;
  }

  /**
   * @return the id
   */
  public String getTransactionId() {
    return transactionId;
  }

  /**
   * @return the session
   */
  public Session getSession() {
    return session;
  }

}
