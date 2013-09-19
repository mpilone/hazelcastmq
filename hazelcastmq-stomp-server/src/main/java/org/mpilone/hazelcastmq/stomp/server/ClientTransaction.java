package org.mpilone.hazelcastmq.stomp.server;

import org.mpilone.hazelcastmq.core.HazelcastMQContext;

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
   * The {@link HazelcastMQContext} used for all message production within the
   * transaction.
   */
  private HazelcastMQContext context;

  /**
   * Constructs the transaction.
   * 
   * @param transactionId
   *          the unique ID of the transaction
   * @param context
   *          the HazelcastMQContext used for all message production within the
   *          transaction
   */
  public ClientTransaction(String transactionId, HazelcastMQContext context) {
    super();
    this.transactionId = transactionId;
    this.context = context;
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
  public HazelcastMQContext getContext() {
    return context;
  }

}
