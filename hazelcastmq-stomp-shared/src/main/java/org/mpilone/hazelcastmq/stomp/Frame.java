package org.mpilone.hazelcastmq.stomp;

import java.util.HashMap;
import java.util.Map;

/**
 * A STOMP frame as defined by the specification.
 * 
 * @author mpilone
 */
public class Frame {
  /**
   * The frame command.
   */
  private Command command;

  /**
   * The map of header keys and values.
   */
  private Map<String, String> headers;

  /**
   * The body of the frame or null if there was no body.
   */
  private byte[] body;

  /**
   * Constructs the frame with no command, headers, or body. A frame is not
   * valid to send until it has a command set.
   */
  public Frame() {
  }

  /**
   * Constructs the frame with the given command.
   * 
   * @param command
   *          the frame command
   */
  public Frame(Command command) {
    this.command = command;
  }

  /**
   * A utility method for setting the content-type header to text/plain.
   */
  public void setContentTypeText() {
    getHeaders().put("content-type", "text/plain");
  }

  /**
   * @return the command
   */
  public Command getCommand() {
    return command;
  }

  /**
   * @param command
   *          the command to set
   */
  public void setCommand(Command command) {
    this.command = command;
  }

  /**
   * Returns the headers of the frame. If no headers have been set, an empty Map
   * is returned.
   * 
   * @return the frame headers
   */
  public Map<String, String> getHeaders() {
    if (headers == null) {
      headers = new HashMap<String, String>();
    }

    return headers;
  }

  /**
   * @param headers
   *          the headers to set
   */
  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  /**
   * @return the body
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * @param body
   *          the body to set
   */
  public void setBody(byte[] body) {
    this.body = body;
  }
}
