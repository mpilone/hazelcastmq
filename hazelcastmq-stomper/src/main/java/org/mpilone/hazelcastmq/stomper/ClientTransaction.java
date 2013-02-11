package org.mpilone.hazelcastmq.stomper;

import javax.jms.Session;

public class ClientTransaction {
  private String transactionId;
  private Session session;

  /**
   * @param transactionId
   * @param session
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
