package org.mpilone.hazelcastmq.core;

import java.nio.charset.Charset;

/**
 * A message to be sent over HazelcastMQ containing headers and a body.
 *
 * @author mpilone
 */
public class HazelcastMQMessage {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private byte[] body;
  private Headers headers;

  public HazelcastMQMessage() {
    this(new DefaultHeaders(), null);
  }

  /**
   * @param headers the headers for the message
   * @param body the body of the message
   */
  public HazelcastMQMessage(Headers headers, byte[] body) {
    super();
    this.body = body;
    this.headers = headers;
  }

  /**
   * @return the destination
   */
  public String getDestination() {
    return getHeaders().get(Headers.DESTINATION);
  }

  /**
   * @param destination
   *          the destination to set
   */
  public void setDestination(String destination) {
    getHeaders().put(Headers.DESTINATION, destination);
  }

  /**
   * @return the contentType
   */
  public String getContentType() {
    return getHeaders().get(Headers.CONTENT_TYPE);
  }

  /**
   * @param contentType
   *          the contentType to set
   */
  public void setContentType(String contentType) {
    getHeaders().put(Headers.CONTENT_TYPE, contentType);
  }

  /**
   * @return the body of the message
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * @param content
   *          the content to set
   */
  public void setBody(byte[] content) {
    this.body = content;
  }

  public void setContentAsString(String content) {
    setBody(content.getBytes(UTF_8));
  }

  public String getContentAsString() {
    return new String(getBody(), UTF_8);
  }

  /**
   * @return the headers
   */
  public Headers getHeaders() {
    if (headers == null) {
      headers = new DefaultHeaders();
    }
    return headers;
  }

  public void setId(String id) {
    getHeaders().put(Headers.MESSAGE_ID, id);
  }

  /**
   * @return the id
   */
  public String getId() {
    return getHeaders().get(Headers.MESSAGE_ID);
  }
}
