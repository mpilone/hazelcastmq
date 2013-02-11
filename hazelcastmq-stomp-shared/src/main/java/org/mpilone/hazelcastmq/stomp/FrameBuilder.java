package org.mpilone.hazelcastmq.stomp;

/**
 * A factory for {@link Frame}s which provides a fluent API for frame
 * construction as well as utility methods for common frame types.
 * 
 * @author mpilone
 */
public class FrameBuilder {
  /**
   * The frame being assembled.
   */
  private Frame frame;

  /**
   * Constructs the frame builder with an empty frame.
   */
  private FrameBuilder() {
    frame = new Frame();
  }

  /**
   * Sets the content-type header to text/plain.
   */
  public FrameBuilder headerContentTypeText() {
    frame.setHeader("content-type", "text/plain");
    return this;
  }

  /**
   * Sets the transaction header to the given ID.
   */
  public FrameBuilder headerTransaction(String transactionId) {
    frame.setHeader("transaction", transactionId);
    return this;
  }

  /**
   * Sets the content-type header to application/octet-stream.
   */
  public FrameBuilder headerContentTypeOctetStream() {
    frame.setHeader("content-type", "application/octet-stream");
    return this;
  }

  /**
   * Creates a frame builder and configures a standard
   * {@link Command#UNSUBSCRIBE} frame.
   * 
   * @param id
   *          the ID of the subscription to unsubscribe
   * @return the frame builder
   */
  public static FrameBuilder unsubscribe(String id) {
    FrameBuilder fb = command(Command.UNSUBSCRIBE).header("id", id).header(
        "ack", "auto");
    return fb;
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#SEND}
   * frame with plain text content. The content type header will be properly
   * set.
   * 
   * @param destination
   *          the destination of the frame (e.g. /queue/foo or /topic/bar)
   * @param body
   *          the body of the frame as a text string
   * @return the frame builder
   */
  public static FrameBuilder send(String destination, String body) {
    FrameBuilder fb = command(Command.SEND).header("destination", destination)
        .headerContentTypeText().body(body);
    return fb;
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#SEND}
   * frame with binary content. The content type header will be properly set.
   * 
   * @param destination
   *          the destination of the frame (e.g. /queue/foo or /topic/bar)
   * @param body
   *          the body of the frame as a text string
   * @return the frame builder
   */
  public static FrameBuilder send(String destination, byte[] body) {
    FrameBuilder fb = command(Command.SEND).header("destination", destination)
        .headerContentTypeOctetStream().body(body);
    return fb;
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#SUBSCRIBE}
   * frame.
   * 
   * @param destination
   *          the destination to subscribe to (e.g. /queue/foo or /topic/bar)
   * @param id
   *          the unique ID of the subscription for tracking
   * @return the frame builder
   */
  public static FrameBuilder subscribe(String destination, String id) {
    FrameBuilder fb = command(Command.SUBSCRIBE)
        .header("destination", destination).header("id", id)
        .header("ack", "auto");
    return fb;
  }

  /**
   * Creates a frame builder and configures the frame with the given command.
   * This method is used as the starting point for frame construction when a
   * more custom frame is needed that what is available from one of the shortcut
   * utility methods.
   * 
   * @param command
   *          the command to set in the frame
   * @return the frame builder
   */
  public static FrameBuilder command(Command command) {
    FrameBuilder fb = new FrameBuilder();
    fb.frame.setCommand(command);
    return fb;
  }

  /**
   * Sets the given header in the frame, replacing the header if it already
   * exists.
   * 
   * @param name
   *          the name of the header
   * @param value
   *          the value of the header
   * @return the frame builder
   */
  public FrameBuilder header(String name, String value) {
    frame.setHeader(name, value);
    return this;
  }

  /**
   * Sets the body of the frame by converting the text into a UTF-8 encoded
   * block of bytes. No content-type header is assumed or set.
   * 
   * @param body
   *          the body to set
   * @return the frame builder
   * @see #headerContentTypeText()
   */
  public FrameBuilder body(String body) {
    return body(body.getBytes(StompConstants.UTF_8));
  }

  /**
   * Sets the body of the frame to the given bytes. No content-type header is
   * assumed or set.
   * 
   * @param body
   *          the body to set
   * @return the frame builder
   * @see #headerContentTypeOctetStream()
   */
  public FrameBuilder body(byte[] body) {
    frame.setBody(body);
    return this;
  }

  /**
   * Builds the final frame and returns it. No further operations should be
   * performed on this frame builder after this method is called.
   * 
   * @return the configured frame
   */
  public Frame build() {
    Frame tmp = frame;
    frame = null;

    return tmp;
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#BEGIN}
   * frame.
   * 
   * @param transactionId
   *          the unique transaction ID
   * @return the frame builder
   */
  public static FrameBuilder begin(String transactionId) {
    return FrameBuilder.command(Command.BEGIN).headerTransaction(transactionId);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#COMMIT}
   * frame.
   * 
   * @param transactionId
   *          the transaction ID
   * @return the frame builder
   */
  public static FrameBuilder commit(String transactionId) {
    return FrameBuilder.command(Command.COMMIT)
        .headerTransaction(transactionId);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#ABORT}
   * frame.
   * 
   * @param transactionId
   *          the transaction ID
   * @return the frame builder
   */
  public static FrameBuilder abort(String transactionId) {
    return FrameBuilder.command(Command.ABORT).headerTransaction(transactionId);
  }

}
