package org.mpilone.stomp;

import java.nio.charset.Charset;

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
  private Headers headers;

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
   * Returns the headers of the frame. If no headers have been set, an empty
   * header map is returned. The map may be modified directly to add and remove
   * headers.
    * 
   * @return the frame headers
   */
  public Headers getHeaders() {
    if (headers == null) {
      headers = new DefaultHeaders();
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
  public void setHeaders(Headers headers) {
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
   * Returns the body as a String using the character set encoding in the
   * content-type header or UTF-8 if no character set is specified in the
   * headers. If there is no body to the message, null is returned. This is just
   * a convenience method for {@link #getBody() }.
   *
   * @return the body as a string or null
   */
  public String getBodyAsString() {
   
    Charset charset = StompConstants.UTF_8;

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
   * Sets the body of the frame by converting the String into UTF-8 encoded
   * bytes. Set the body to null to clear it. This is just a convenience method
   * for {@link #setBody(byte[]) }.
   *
   * @param body the body or null
   */
  public void setBodyAsString(String body) {
    if (body == null) {
      this.body = null;
    }
    else {
      setBody(body.getBytes(StompConstants.UTF_8));
    }
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

  @Override
  public String toString() {
    return "Frame{" + "command=" + command + ", headers=" + headers + ", body="
        + (body != null ? body.length + " bytes" : "null") + '}';
  }
}
