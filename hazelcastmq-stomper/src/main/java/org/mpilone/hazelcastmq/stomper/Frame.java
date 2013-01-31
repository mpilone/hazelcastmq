package org.mpilone.hazelcastmq.stomper;

import java.util.HashMap;
import java.util.Map;

public class Frame {
  private Command command;
  private Map<String, String> headers;
  private byte[] body;

  public Frame() {
  }

  public Frame(Command command) {
    this.command = command;
  }

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
   * @return the headers
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
