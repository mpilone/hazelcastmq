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
   * Returns the frame command or null if not set.
   * 
   * @return the command or null
   */
  public Command getCommand() {
    return command;
  }

  /**
   * Sets the frame command, replacing any previously set command. A frame must
   * have a command before it can be sent.
   * 
   * @param command
   *          the command to set
   */
  public void setCommand(Command command) {
    this.command = command;
  }

  /**
   * Returns the headers of the frame. If no headers have been set, an empty Map
   * is returned. The map may be modified directly to add and remove headers.
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
   * Sets a single header, replacing the value if it already exists.
   * 
   * @param name
   *          the name of the header
   * @param value
   *          the new value
   */
  public void setHeader(String name, String value) {
    getHeaders().put(name, value);
  }

  /**
   * Sets all the headers for the frame, replacing any existing headers.
   * 
   * @param headers
   *          the headers to set
   */
  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  /**
   * Returns the body of the frame, or null if there is no body.
   * 
   * @return the body of the frame
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * Sets the body of the frame. Set the body to null to clear it.
   * 
   * @param body
   *          the body or null
   */
  public void setBody(byte[] body) {
    this.body = body;
  }
}
