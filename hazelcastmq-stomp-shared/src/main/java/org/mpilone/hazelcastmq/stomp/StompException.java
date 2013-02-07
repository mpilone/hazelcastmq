package org.mpilone.hazelcastmq.stomp;

/**
 * An exception related to STOMP frame processing.
 * 
 * @author mpilone
 */
public class StompException extends RuntimeException {

  /**
   * Serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Constructs the exception with the given message.
   * 
   * @param message
   *          the human readable message
   */
  public StompException(String message) {
    super(message);
  }

  /**
   * Constructs the exception with the given cause.
   * 
   * @param cause
   *          the root cause exception
   */
  public StompException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs the exception with the given message and cause.
   * 
   * @param message
   *          the human readable message
   * @param cause
   *          the root cause exception
   */
  public StompException(String message, Throwable cause) {
    super(message, cause);
  }

}
