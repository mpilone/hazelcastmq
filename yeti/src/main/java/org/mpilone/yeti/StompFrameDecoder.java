package org.mpilone.yeti;

import static java.lang.String.format;
import static org.mpilone.yeti.StompConstants.*;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.*;

/**
 * A STOMP frame decoder that processes raw bytes into {@link Frame} instances.
 *
 * @author mpilone
 */
public class StompFrameDecoder extends ReplayingDecoder<StompFrameDecoder.DecoderState> {

  /**
   * The default maximum frame size in bytes. The default value is 128 KiB.
   */
  public static final int DEFAULT_MAX_FRAME_SIZE = 128 * 1024;

  private int totalDecodedByteCount;
  private int currentDecodedByteCount;
  private Command command;
  private DefaultHeaders headers;
  private byte[] body;
  private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

  /**
   * A "magic" header that indicates that the frame was poorly formatted. If set
   * on a frame, the contents of the frame should be considered invalid and an
   * error should probably be sent to the client. The value of the header will
   * be a simple error message describing the failure.
   */
  public static final String HEADER_BAD_REQUEST = StompFrameDecoder.class.
      getName() + "::BAD_REQUEST";

  /**
   * Constructs the decoder with the default maximum frame size.
   */
  public StompFrameDecoder() {
    this(DEFAULT_MAX_FRAME_SIZE);
  }

  /**
   * Constructs the decoder with the given maximum frame size.
   *
   * @param maxFrameSize the maximum size of a single FRAME
   */
  public StompFrameDecoder(int maxFrameSize) {
    super(DecoderState.READ_CONTROL_CHARS);

    this.maxFrameSize = maxFrameSize;
    reset();
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {

    DecoderState nextState;

    // Reset the bytes read for this single decode pass.
    currentDecodedByteCount = 0;

    try {
      switch (state()) {
        case READ_CONTROL_CHARS:
          nextState = readControlChars(in);
          checkpoint(nextState);
          break;

        case READ_COMMAND:
          nextState = readCommand(in);
          checkpoint(nextState);
          break;

        case READ_HEADERS:
          nextState = readHeaders(in);
          checkpoint(nextState);
          break;

        case READ_BODY:
          nextState = readBody(in);
          if (nextState != DecoderState.READ_BODY) {
            // Found the end of the body. Build the frame and reset.
            out.add(buildFrame());
            reset();
          }
          checkpoint(nextState);
          break;

        case DISCARD_FRAME:
          nextState = readAndDiscard(in);
          if (nextState != DecoderState.DISCARD_FRAME) {
            reset();
          }
          checkpoint(nextState);
          break;

        default:
          // This should never happen unless there is a bug in the decoder.
          throw new IllegalStateException("Unknown state: " + state());
      }

      totalDecodedByteCount += currentDecodedByteCount;
    }
    catch (CorruptedFrameException | TooLongFrameException ex) {
      out.add(buildFrame(ex));
      reset();
      checkpoint(DecoderState.DISCARD_FRAME);
    }
  }

  /**
   * <p>
   * Reads a single line of UTF-8 text from the stream and returns once an EOL
   * is found. STOMP defines the EOL as a new line character with an optional
   * leading carriage return character. The EOL character(s) will be removed
   * from the returned data. If no EOL character is found, this method does not
   * read any data from the input buffer.
   * </p>
   * <p>
   * This method checks for too long frames.
   * </p>
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state or null if no EOL is available in the buffer
   */
  private String readLine(ByteBuf in) {

    int bytesToRead;
    byte[] data = null;
    int bytesToSkip;

    if ((bytesToRead = in.bytesBefore((byte) LINE_FEED_CHAR)) > -1) {
      // Found the line feed.
      bytesToSkip = 1;

      // Check (and ignore) optional carriage return.
      if (bytesToRead > 0 && in.getByte(bytesToRead - 1) == CARRIAGE_RETURN_CHAR) {
        bytesToSkip++;
        bytesToRead--;
      }

      // Check that the bytes we're about to read will not exceed the
      // max frame size.
      checkTooLongFrame(bytesToRead);

      data = new byte[bytesToRead];
      in.readBytes(data);
      in.skipBytes(bytesToSkip);

      // Count the bytes read.
      currentDecodedByteCount += bytesToRead;
    }
    else {
      // No line feed. Make sure we're not buffering more than the max
      // frame size.
      checkTooLongFrame(in.readableBytes());
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
   * @return the next decoder state
   */
  private DecoderState readBody(ByteBuf in) {

    int bytesToRead;
    DecoderState nextState = DecoderState.READ_BODY;

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

    // Make sure the buffer or the number of bytes to read is less than
    // the max frame size.
    checkTooLongFrame(bytesToRead == -1 ? in.readableBytes() : bytesToRead);

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

      currentDecodedByteCount += bytesToRead;
      nextState = DecoderState.READ_CONTROL_CHARS;
    }

    return nextState;
  }

  /**
   * Reads the headers of the frame if available.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state
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

    DecoderState nextState = DecoderState.READ_CONTROL_CHARS;

    int index = in.forEachByte(new ByteBufProcessor() {
      @Override
      public boolean process(byte b) throws Exception {
        switch (b) {
          // This is a little more lax than the spec which allows for only
          // EOL character(s) between frames.
          case ' ':
          case CARRIAGE_RETURN_CHAR:
          case LINE_FEED_CHAR:
          case NULL_CHAR:
            // ignore the character
            return true;

          default:
            return false;
        }
      }
    });

    if (index != -1) {
      // A non-control character was found so we skip up to that index and
      // move to the next state.
      in.readerIndex(index);
      nextState = DecoderState.READ_COMMAND;
    }
    else {
      // Discard all available bytes because we couldn't find a
      // non-control character.
      in.readerIndex(in.writerIndex());
    }

    return nextState;
  }

  /**
   * Reads the command of the frame.
   *
   * @param in the input buffer to read from
   *
   * @return the next decoder state
   */
  private DecoderState readCommand(ByteBuf in) {

    DecoderState nextState = DecoderState.READ_COMMAND;
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
    currentDecodedByteCount = 0;
    totalDecodedByteCount = 0;
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

    int byteCount = in.bytesBefore((byte) NULL_CHAR);

    if (byteCount > -1) {
      // Skip all the bytes including the null character and move to the next
      // state.
      in.skipBytes(byteCount + 1);
      nextState = DecoderState.READ_CONTROL_CHARS;
    }
    else {
      // Clear the buffer, discarding all the data before the future null
      // character.
      in.readerIndex(in.writerIndex());
    }

    return nextState;
  }

  /**
   * Checks if the current frame is too long and generates a
   * {@link TooLongFrameException} if decoding should stop. The total compared
   * is the sum of the {@code expectedToRead},
   * {@link #totalDecodedByteCount}, {@link #currentDecodedByteCount}.
   *
   * @param expectedToRead the number of bytes expected to be read in the
   * current frame
   *
   * @throws TooLongFrameException if the current frame is larger than the
   * configured maximum
   */
  private void checkTooLongFrame(int expectedToRead) throws
      TooLongFrameException {

    int total = expectedToRead + totalDecodedByteCount + currentDecodedByteCount;

    if (total > maxFrameSize) {
      throw new TooLongFrameException(format(
          "Frame size [%d] is larger than the maximum frame size [%d]. "
          + "Decoding will be aborted.", total, maxFrameSize));
    }
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
