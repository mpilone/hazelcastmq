package org.mpilone.yeti;

/**
 * An exception related to STOMP frame processing. A client exception will be
 * relayed to the connected STOMP client as an ERROR frame using the exceptions
 * message and details.
  *
 * @author mpilone
 */
public class StompClientException extends StompException {

  /**
   * Serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Constructs the exception with a related frame that was being processed when
   * the error occurred.
   *
   * @param message the short error message
   * @param details the detailed error message to include in a response body
   * @param frame the frame that was being processed when the error occurred
   * @param cause the root cause exception
   */
  public StompClientException(String message, String details, Frame frame,
      Throwable cause) {
    super(message, details, frame, cause);
  }

  /**
   * Constructs the exception with a related frame that was being processed when
   * the error occurred.
   *
   * @param message the short error message
   * @param details the detailed error message to include in a response body
   * @param frame the frame that was being processed when the error occurred
   */
  public StompClientException(String message, String details, Frame frame) {
    super(message, details, frame);
  }

  /**
   * Constructs the exception with the given message.
   * 
   * @param message
   *          the human readable message
   */
  public StompClientException(String message) {
    super(message);
  }

  /**
   * Constructs the exception with the given cause.
   * 
   * @param cause
   *          the root cause exception
   */
  public StompClientException(Throwable cause) {
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
  public StompClientException(String message, Throwable cause) {
    super(message, cause);
  }

}
