package org.mpilone.yeti;

/**
 * A factory for {@link Frame}s which provides a fluent API for frame
 * construction as well as utility methods for common frame types.
 * 
 * @author mpilone
 */
public class FrameBuilder {

  private byte[] body;
  private final DefaultHeaders headers;
  private Command command;

  /**
   * Constructs the frame builder with an empty frame.
   */
  private FrameBuilder() {
    headers = new DefaultHeaders();
  }

  /**
   * Creates a frame builder populated with all the values of the source frame.
   *
   * @param source the source frame to copy
   *
   * @return the populated frame builder
   */
  public static FrameBuilder copy(Frame source) {
    FrameBuilder fb = command(source.getCommand());
    fb.body = source.getBody();

    Headers headers = source.getHeaders();
    if (headers != null) {
      fb.headers.putAll(headers.getHeaderMap());
    }

    return fb;
  }

  /**
   * Sets the content-type header to text/plain.
   *
   * @return the frame builder
   */
  public FrameBuilder headerContentTypeText() {
    headers.put(Headers.CONTENT_TYPE, "text/plain");
    return this;
  }

  /**
   * Sets the content-type header to application/octet-stream.
   *
   * @return the frame builder
   */
  public FrameBuilder headerContentTypeOctetStream() {
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");
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
    return command(Command.UNSUBSCRIBE).header(Headers.ID, id).
        header(Headers.ACK, "auto");
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#RECEIPT}
   * frame.
   *
   * @param receiptId the ID of the receipt
   *
   * @return the frame builder
   */
  public static FrameBuilder receipt(String receiptId) {
    return command(Command.RECEIPT).header(Headers.RECEIPT_ID, receiptId);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#SEND}
   * frame with plain text content. The content-length header will be set and
   * the content-type header will be set to text/plain.
    *
   * @param destination
   *          the destination of the frame (e.g. /queue/foo or /topic/bar)
   * @param body
   *          the body of the frame as a text string or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder send(String destination, String body) {
    byte[] bodyBytes = body == null ? null : body.getBytes(StompConstants.UTF_8);

    return send(destination, bodyBytes).headerContentTypeText();
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#SEND}
   * frame with binary content. The content-length header will be set and the
   * content-type header will be set to application/octet-stream.
    *
   * @param destination
   *          the destination of the frame (e.g. /queue/foo or /topic/bar)
   * @param body
   *          the body of the frame as a text string or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder send(String destination, byte[] body) {
    return command(Command.SEND).header(Headers.DESTINATION,
        destination).headerContentTypeOctetStream().body(body).
        headerContentLength();
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
        .header(Headers.DESTINATION, destination).header(Headers.ID, id)
        .header(Headers.ACK, "auto");
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
    fb.command = command;
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
    headers.put(name, value);
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
   * Sets the content-length header to the length of the body or removes the
   * header if no body has been set.
   *
   * @return the frame builder
   */
  public FrameBuilder headerContentLength() {
    if (body != null) {
      header(Headers.CONTENT_LENGTH, String.valueOf(body.length));
    }
    else {
      headers.remove(Headers.CONTENT_LENGTH);
    }

    return this;
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
    this.body = body;
    return this;
  }

  /**
   * Builds the final frame and returns it. No further operations should be
   * performed on this frame builder after this method is called.
   * 
   * @return the configured frame
   */
  public Frame build() {
    return new Frame(command, headers, body);
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
    return FrameBuilder.command(Command.BEGIN).header(Headers.TRANSACTION,
        transactionId);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#ERROR}
   * frame. The content-length header will be set and the content-type header
   * will be set to text/plain.
   *
   * @param body the body of the error frame or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder error(String body) {
    byte[] bodyBytes = body == null ? null : body.getBytes(StompConstants.UTF_8);

    return error(bodyBytes).headerContentTypeText();
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#ERROR}
   * frame. The content-length header will be set and the content-type header
   * will be set to application/octet-stream.
   *
   * @param body the body of the error frame or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder error(byte[] body) {
    return FrameBuilder.command(Command.ERROR).body(body).headerContentLength().
        headerContentTypeText();
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#MESSAGE}
   * frame. The content-length header will be set and the content-type header
   * will be set to text/plain.
   *
   * @param destination the destination the message was sent to
   * @param messageId unique identifier for the message
   * @param subscription the identifier of the subscription that is receiving
   * the message
   * @param body the body of the frame or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder message(String destination, String messageId,
      String subscription, String body) {
    byte[] bodyBytes = body == null ? null : body.getBytes(StompConstants.UTF_8);

    return message(destination, messageId, subscription, bodyBytes).
        headerContentTypeText();
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#MESSAGE}
   * frame. The content-length header will be set and the content-type header
   * will be set to application/octet-stream.
   *
   * @param destination the destination the message was sent to
   * @param messageId unique identifier for the message
   * @param subscription the identifier of the subscription that is receiving
   * the message
   * @param body the body of the frame or null for no body
   *
   * @return the frame builder
   */
  public static FrameBuilder message(String destination, String messageId,
      String subscription, byte[] body) {

    return FrameBuilder.command(Command.MESSAGE).body(body).header(
        Headers.DESTINATION, destination).header(Headers.MESSAGE_ID, messageId).
        header(Headers.SUBSCRIPTION, subscription).headerContentLength().
        headerContentTypeOctetStream();
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
        .header(Headers.TRANSACTION, transactionId);
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
    return FrameBuilder.command(Command.ABORT).header(Headers.TRANSACTION,
        transactionId);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#CONNECTED}
   * frame.
   *
   * @param version the highest version of the protocol in common with the
   * client
   *
   * @return the frame builder
   */
  public static FrameBuilder connected(String version) {
    return FrameBuilder.command(Command.CONNECTED).header(Headers.VERSION,
        version);
  }

  /**
   * Creates a frame builder and configures a standard {@link Command#CONNECTED}
   * frame.
   *
   * @param acceptVersion the versions of the STOMP protocol the client supports
   * @param host the name of a virtual host that the client wishes to connect to
   *
   * @return the frame builder
   */
  public static FrameBuilder connect(String acceptVersion, String host) {
    return FrameBuilder.command(Command.CONNECT).
        header(Headers.ACCEPT_VERSION, acceptVersion).header(Headers.HOST, host);
  }

  /**
   * Creates a frame builder and configures a standard
   * {@link Command#DISCONNECT} frame.
   *
   * @return the frame builder
   */
  public static FrameBuilder disconnect() {
    return FrameBuilder.command(Command.DISCONNECT);
  }

}
