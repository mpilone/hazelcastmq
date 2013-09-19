package org.mpilone.hazelcastmq.core;

public class HazelcastMQException extends RuntimeException {

  /**
   * Serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param arg0
   * @param arg1
   */
  public HazelcastMQException(String arg0, Throwable arg1) {
    super(arg0, arg1);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param arg0
   */
  public HazelcastMQException(String arg0) {
    super(arg0);
    // TODO Auto-generated constructor stub
  }

}
