package org.mpilone.yeti;

import java.nio.charset.Charset;

/**
 * A STOMP frame as defined by the specification. Frames are immutable and
 * should be constructed (or duplicated) using a {@link FrameBuilder}.
  *
 * @author mpilone
 */
public class Frame {

  /**
   * The frame command.
   */
  private final Command command;

  /**
   * The map of header keys and values.
   */
  private final Headers headers;

  /**
   * The body of the frame or null if there was no body.
   */
  private final byte[] body;

  public Frame(Command command, Headers headers, byte[] body) {
    this.command = command;
    this.headers = headers == null ? new DefaultHeaders() : headers;
    this.body = body;
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
   * Returns the headers of the frame. If no headers have been set, an empty
   * header map is returned. The map may be modified directly to add and remove
   * headers.
    * 
   * @return the frame headers
   */
  public Headers getHeaders() {
    

    return headers;
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

  @Override
  public String toString() {
    return "Frame{" + "command=" + command + ", headers=" + headers + ", body="
        + (body != null ? body.length + " bytes" : "null") + '}';
  }
}
