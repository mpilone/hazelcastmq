package org.mpilone.yeti;

import static org.mpilone.yeti.StompConstants.*;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.ReplayingDecoder;

/**
 * A STOMP frame decoder that processes raw bytes into {@link Frame} instances.
 *
 * TODO: enforce a maximum frame size and header size
 *
 * @author mpilone
 */
public class StompFrameDecoder extends ReplayingDecoder<StompFrameDecoder.DecoderState> {

  private Command command;
  private DefaultHeaders headers;
  private byte[] body;

  /**
   * A "magic" header that indicates that the frame was poorly formatted. If set
   * on a frame, the contents of the frame should be considered invalid and an
   * error should probably be sent to the client. The value of the header will
   * be a simple error message describing the failure.
   */
  public static final String HEADER_BAD_REQUEST = StompFrameDecoder.class.
      getName() + "::BAD_REQUEST";

  /**
   * Constructs the decoder with an initial state.
   */
  public StompFrameDecoder() {
    super(DecoderState.READ_CONTROL_CHARS);

    reset();
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {

    DecoderState nextState;

    try {
      switch (state()) {
        case READ_CONTROL_CHARS:
          nextState = readControlChars(in);
          checkpoint(nextState);
          break;

        case READ_COMMAND:
          nextState = readCommand(in);
          if (nextState != null) {
            checkpoint(nextState);
          }
          break;

        case READ_HEADERS:
          nextState = readHeaders(in);
          if (nextState != null) {
            checkpoint(nextState);
          }
          break;

        case READ_BODY:
          nextState = readBody(in);
          if (nextState != null && nextState != DecoderState.READ_BODY) {
            // The frame is done because we're moving to a new state.
            out.add(buildFrame());
            reset();
            checkpoint(nextState);
          }
          break;

        case DISCARD_FRAME:
          nextState = readAndDiscard(in);
          checkpoint(nextState);
          break;

        default:
          // This should never happen unless there is a bug in the decoder.
          throw new IllegalStateException("Unknown state: " + state());
      }
    }
    catch (Exception ex) {
      out.add(buildFrame(ex));
      reset();
      checkpoint(DecoderState.DISCARD_FRAME);
    }
  }

  /**
   * Reads a single line of UTF-8 text from the stream and returns once an EOL
   * is found. STOMP defines the EOL as a new line character with an optional
   * leading carriage return character. The EOL character(s) will be removed
   * from the returned data.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no checkpoint should be set
   */
  private String readLine(ByteBuf in) {

    int bytesToRead;
    byte[] data = null;
    int bytesToSkip = 0;

    // Look for the line feed.
    if ((bytesToRead = in.bytesBefore((byte) LINE_FEED_CHAR)) > -1) {
      bytesToSkip = 1;

      // Check (and ignore) optional carriage return.
      if (bytesToRead > 0 && in.getByte(bytesToRead - 1) == CARRIAGE_RETURN_CHAR) {
        bytesToSkip++;
        bytesToRead--;
      }

      data = new byte[bytesToRead];
      in.readBytes(data);
      in.skipBytes(bytesToSkip);
    }

    if (data != null) {
      return new String(data, StompConstants.UTF_8);
    }
    else {
      return null;
    }
  }

  /**
   * Reads the body of the frame using the content-length header if available.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no checkpoint should be set
   */
  private DecoderState readBody(ByteBuf in) {

    int bytesToRead;
    DecoderState nextState = null;

    // See if we have a content-length header.
    if (headers.getHeaderNames().contains(Headers.CONTENT_LENGTH)) {
      // Read the number of bytes specified in the content-length header.
      bytesToRead = Integer.valueOf(headers.get(Headers.CONTENT_LENGTH));

      // If we don't have enough bytes yet we won't try to read anything.
      if (in.readableBytes() < bytesToRead + 1) {
        bytesToRead = -1;
      }
    }
    else {
      bytesToRead = in.bytesBefore((byte) NULL_CHAR);
    }

    // We want to avoid creating a new byte[] for the body for each call if the 
    // full body isn't available yet. Therefore we'll simply not read anything
    // if we don't have all the data we need yet.
    if (bytesToRead > -1) {

      // An empty body is valid. If so, we'll just leave the body null
      // in the frame.
      if (bytesToRead > 0) {
        byte[] data = new byte[bytesToRead];
        in.readBytes(data, 0, data.length);
        body = data;
      }

      // Sanity check that the frame ends appropriately.
      if (in.readByte() != NULL_CHAR) {
        throw new CorruptedFrameException("Frame must end with NULL character.");
      }

      nextState = DecoderState.READ_CONTROL_CHARS;
    }

    return nextState;
  }

  /**
   * Reads the headers of the frame if available.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no checkpoint should be set
   */
  private DecoderState readHeaders(ByteBuf in) {

    DecoderState nextState = DecoderState.READ_HEADERS;
    String line;

    // Read as long as we haven't reached the end of the headers (i.e.
    // the next state) and we have full lines to read.
    while (nextState == DecoderState.READ_HEADERS && (line = readLine(in))
        != null) {

      if (line.isEmpty()) {
        nextState = DecoderState.READ_BODY;
      }
      else {
        int pos = line.indexOf(COLON_CHAR);
        if (pos > 0) {
          String key = line.substring(0, pos);

          // The spec defines that only the first occurrance of a header
          // should be preserved in a single frame.
          if (!headers.getHeaderNames().contains(key)) {

            // Move past the colon delimiter.
            pos++;

            // Extract the value and decode header value as per the spec. Is
            // there a faster way to do this than a bunch of individual
            // replaces?
            String value = pos >= line.length() ? null : line.substring(pos).
                replace(OCTET_92_92, OCTET_92)
                .replace(OCTET_92_99, OCTET_58)
                .replace(OCTET_92_110, OCTET_10)
                .replace(OCTET_92_114, OCTET_13);

            headers.put(key, value);
          }
        }
        else {
          // Invalid frame. A header must contain a ':'.
          throw new CorruptedFrameException("Header must contain a name and "
              + "value separated by a colon character.");
        }
      }
    }

    return nextState;
  }

  /**
   * Reads the optional EOL (and other control characters) that are permitted
   * between the end of one frame and the start of the next frame. When a
   * non-control character is detected, the decoder state will be advanced.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no checkpoint should be set
   */
  private DecoderState readControlChars(ByteBuf in) {

    int pos;
    DecoderState nextState = DecoderState.READ_CONTROL_CHARS;
    for (pos = 0; pos < in.readableBytes() && nextState
        == DecoderState.READ_CONTROL_CHARS; ) {

      byte b = in.getByte(pos);

      switch (b) {
        // This is a little more lax than the spec which allows for only
        // EOL character(s) between frames.
        case ' ':
        case CARRIAGE_RETURN_CHAR:
        case LINE_FEED_CHAR:
        case NULL_CHAR:
          // ignore the character
          pos++;
          break;

        default:
          nextState = DecoderState.READ_COMMAND;
          break;
      }
    }

    in.skipBytes(pos);

    return nextState;
  }

  /**
   * Reads the command of the frame.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no checkpoint should be set
   */
  private DecoderState readCommand(ByteBuf in) {

    DecoderState nextState = null;
    String line = readLine(in);

    if (line != null) {
      command = Command.valueOf(line);
      nextState = DecoderState.READ_HEADERS;
    }

    return nextState;
  }

  /**
   * Resets the command, headers, and body for the next frame.
   */
  private void reset() {
    // Reset to the inital state.
    command = null;
    headers = new DefaultHeaders();
    body = null;
  }

  /**
   * Builds a frame using the currently read command, headers, and body. A
   * header with the name {@link #HEADER_BAD_REQUEST} will be added indicating
   * that the frame is probably not valid and should be handled appropriately.
   *
   * @param ex the exception that caused the frame to be considered invalid
   *
   * @return the new frame
   */
  private Frame buildFrame(Exception ex) {
    headers.put(HEADER_BAD_REQUEST, ex.getMessage());

    if (command == null) {
      // If we don't have a command yet we'll have to default to something
      // in order to construct a frame. I don't like this solution but I'm
      // hesitent to relax the validation in the frame constructor or
      // introduce another command type.
      command = Command.ERROR;
    }

    return new Frame(command, headers, body);
  }

  /**
   * Builds a frame using the currently read command, headers, and body.
   *
   * @return the new frame
   */
  private Frame buildFrame() {
    return new Frame(command, headers, body);
  }

  /**
   * Reads any bytes in the buffer and discards them. When a {@link #NULL_CHAR}
   * is reached, the state will be reset for the start of the next frame. This
   * method is normally only used when a bad frame has been detected and should
   * be skipped; however in most cases if a bad frame is received a later
   * handler will send an error frame and terminate the connection per the STOMP
   * specification.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state
   */
  private DecoderState readAndDiscard(ByteBuf in) {

    DecoderState nextState = DecoderState.DISCARD_FRAME;

    while (nextState == DecoderState.DISCARD_FRAME && in.readableBytes() > 0) {
      byte b = in.readByte();

      if (b == NULL_CHAR) {
        nextState = DecoderState.READ_CONTROL_CHARS;
      }
    }

    return nextState;
  }

  /**
   * The various frame parsing states when decoding a STOMP frame.
   */
  enum DecoderState {

    READ_CONTROL_CHARS,
    READ_COMMAND,
    READ_HEADERS,
    READ_BODY,
    DISCARD_FRAME;
  }

}
