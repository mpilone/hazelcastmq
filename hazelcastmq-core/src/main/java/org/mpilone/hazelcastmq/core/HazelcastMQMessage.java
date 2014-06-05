package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * A message to be sent over HazelcastMQ containing headers and a body.
 *
 * @author mpilone
 */
public class HazelcastMQMessage implements Serializable {

  /**
   * The UTF-8 character set for easy reference.
   */
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * The body of the message.
   */
  private byte[] body;

  /**
   * The headers of the message.
   */
  private Headers headers;

  /**
   * Construct a message with no body and empty headers. This method is
   * primarily used by serialization.
   */
  public HazelcastMQMessage() {
    this(new DefaultHeaders(), null);
  }

  /**
   * Constructs a message with the given headers and body.
   *
   * @param headers the headers for the message
   * @param body the body of the message
   */
  public HazelcastMQMessage(Headers headers, byte[] body) {
    super();
    this.body = body;
    this.headers = headers;
  }

  /**
   * Returns the destination of the message. This is a convenience method for
   * getting the {@link Headers#DESTINATION} value.
   *
   * @return the destination
   */
  public String getDestination() {
    return getHeaders().get(Headers.DESTINATION);
  }

  /**
   * Sets the destination of the message. This is a convenience method for
   * setting the {@link Headers#DESTINATION} value.
   *
   * @param destination
   *          the destination to set
   */
  public void setDestination(String destination) {
    getHeaders().put(Headers.DESTINATION, destination);
  }

  /**
   * Returns the content type of the message. This is a convenience method for
   * getting the {@link Headers#CONTENT_TYPE} value.
   *
   * @return the content type of the message
   */
  public String getContentType() {
    return getHeaders().get(Headers.CONTENT_TYPE);
  }

  /**
   * Sets the content type of the message. This is a convenience method for
   * setting the {@link Headers#CONTENT_TYPE} value.
   *
   * @param contentType
   *          the content type to set
   */
  public void setContentType(String contentType) {
    getHeaders().put(Headers.CONTENT_TYPE, contentType);
  }

  /**
   * Returns the body of the message which may be null if no body has been set.
   *
   * @return the body of the message
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * Sets the body of the message to the given content.
   *
   * @param content
   *          the body content to set or null to clear the body
   */
  public void setBody(byte[] content) {
    this.body = content;
  }

  /**
   * Sets the body as a String. The String will be converted to bytes using
   * UTF-8 encoding.
   *
   * @param content the body content to set
   */
  public void setBody(String content) {
    if (content != null) {
      setBody(content.getBytes(UTF_8));
    }
    else {
      setBody((byte[]) null);
    }
  }

  /**
   * Returns the body as a String using the character set encoding in the
   * content-type header or UTF-8 if no character set is specified in the
   * headers. If there is no body to the message, null is returned. This is just
   * a convenience method for {@link #getBody() }.
   *
   * @return the body as a string or null
   */
  public String getBodyAsString() {

    Charset charset = UTF_8;

    String contentType = getHeaders().get(Headers.CONTENT_TYPE);
    if (contentType != null && contentType.contains(";")) {
      String[] parts = contentType.split(";");
      for (String part : parts) {
        if (part.startsWith("charset=")) {
          charset = Charset.forName(part.substring("charset=".length()));
        }
      }
    }

    return getBodyAsString(charset);
  }

  /**
   * Returns the body as a String with the given character set encoding. If
   * there is no body to the message, null is returned. This is just a
   * convenience method for {@link #getBody() }.
   *
   * @param charset the character set encoding
   *
   * @return the body as a string or null
   */
  public String getBodyAsString(Charset charset) {
    if (body != null) {
      return new String(body, charset);
    }
    else {
      return null;
    }
  }

  /**
   * Returns the headers of the message.
   *
   * @return the headers the headers of the message
   */
  public Headers getHeaders() {
    if (headers == null) {
      headers = new DefaultHeaders();
    }
    return headers;
  }

  /**
   * Sets the ID of the message. This is a convenience method for setting the
   * {@link Headers#MESSAGE_ID} value.
   *
   * @param id the ID of the message
   */
  public void setId(String id) {
    getHeaders().put(Headers.MESSAGE_ID, id);
  }

  /**
   * Returns the ID of the message. This is a convenience method for getting the
   * {@link Headers#MESSAGE_ID} value.
   *
   * @return the id of the message
   */
  public String getId() {
    return getHeaders().get(Headers.MESSAGE_ID);
  }

  /**
   * Sets the correlation ID of the message. This is a convenience method for
   * setting the {@link Headers#CORRELATION_ID} value.
   *
   * @param correlationId the correlation ID of the message
   */
  public void setCorrelationId(String correlationId) {
    getHeaders().put(Headers.CORRELATION_ID, correlationId);
  }

  /**
   * Returns the correlation ID of the message. This is a convenience method for
   * getting the {@link Headers#CORRELATION_ID} value.
   *
   * @return the correlation ID of the message
   */
  public String getCorrelationId() {
    return getHeaders().get(Headers.CORRELATION_ID);
  }
  /**
   * Sets the reply to destination of the message. This is a convenience method
   * for setting the {@link Headers#REPLY_TO} value.
   *
   * @param destination the reply to destination
   */
  public void setReplyTo(String destination) {
    getHeaders().put(Headers.REPLY_TO, destination);
  }

  /**
   * Returns the reply to destination of the message. This is a convenience
   * method for getting the {@link Headers#REPLY_TO} value.
   *
   * @return the reply to destination
   */
  public String getReplyTo() {
    return getHeaders().get(Headers.REPLY_TO);
  }
}
