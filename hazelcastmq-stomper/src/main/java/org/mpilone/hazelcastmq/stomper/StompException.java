package org.mpilone.hazelcastmq.stomper;

/**
 * @author mpilone
 * 
 */
public class StompException extends RuntimeException {

  /**
   * @param message
   */
  public StompException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public StompException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public StompException(String message, Throwable cause) {
    super(message, cause);
  }

}
