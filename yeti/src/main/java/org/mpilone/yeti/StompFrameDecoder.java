package org.mpilone.yeti;

import static org.mpilone.yeti.StompConstants.*;

import java.io.*;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

/**
 * TODO: enforce a maximum message size and header size
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
    super(DecoderState.READ_COMMAND);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {

    switch (state()) {
      case READ_COMMAND:
        if (readCommand(in)) {
          checkpoint(DecoderState.READ_HEADERS);
        }
        break;

      case READ_HEADERS:
        if (readHeaders(in)) {
          checkpoint(DecoderState.READ_BODY);
        }
        break;

      case READ_BODY:
        if (readBody(in)) {
          out.add(new Frame(command, headers, body));

          // Reset to the inital state.
          command = null;
          headers = null;
          body = null;

          checkpoint(DecoderState.READ_COMMAND);
        }
        break;

      default:
        // This should never happen unless there is a bug in the decoder.
        throw new IllegalStateException("Unknown state: " + state());
    }
  }

  /**
   * Reads a single line of UTF-8 text from the stream and returns once a new
   * line character is found.
   *
   * @return the line of text read or null if there is no line available in the
   * buffer
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

    }
    // Look for the null terminator which could indicate an
    // empty (heart-beat frame).
    else if ((bytesToRead = in.bytesBefore((byte) NULL_CHAR)) > -1) {
      bytesToSkip = 1;

      data = new byte[bytesToRead];
      in.readBytes(data);
    }

    in.skipBytes(bytesToSkip);

    if (data != null) {
      return new String(data, StompConstants.UTF_8);
    }
    else {
      return null;
    }
  }

  /**
   * Reads the body of the frame using the content-length header if available.
   * The body will be set in the frame after reading.
   *
   * @param frame the partial frame that has been read to this point
   *
   * @throws IOException if there is an error reading from the underlying stream
   */
  private boolean readBody(ByteBuf in) {

    int bytesToRead;
    boolean eob = false;

    // See if we have a content-length header.
    if (headers.getHeaderNames().contains(Headers.CONTENT_LENGTH)) {
      // Read the number of bytes specified in the content-length header.
      bytesToRead = Integer.valueOf(headers.get(Headers.CONTENT_LENGTH));

      // If we don't have enough bytes yet we won't try to read anything.
      if (in.readableBytes() < bytesToRead) {
        bytesToRead = -1;
      }
    }
    else {
      bytesToRead = in.bytesBefore((byte) NULL_CHAR);
    }

    if (bytesToRead > -1) {

      if (bytesToRead > 0) {
        byte[] data = new byte[bytesToRead];
        in.readBytes(data, 0, data.length);
        body = data;
      }

      // Sanity check that the frame ends appropriately.
      if (in.readByte() != NULL_CHAR) {
        headers.put(HEADER_BAD_REQUEST,
            "Frame must end with NULL character.");
      }
      
      eob = true;
    }

    return eob;
  }

  /**
   * Reads the headers of the frame if available. The headers will be set in the
   * frame after reading.
   *
   * @param frame the partial frame that has been read to this point
   *
   * @throws IOException if there is an error reading from the underlying stream
   */
  private boolean readHeaders(ByteBuf in) throws IOException {

    headers = new DefaultHeaders();

    // Read until we find a blank line (i.e. end of headers).
    boolean eoh = false;
    while (!eoh) {
      String line = readLine(in);

      if (line == null) {
        break;
      }
      else if (line.isEmpty()) {
        eoh = true;
      }
      else {
        int pos = line.indexOf(COLON_CHAR);
        if (pos > 0) {
          String key = line.substring(0, pos);
          String value = line.substring(pos + 1, line.length());

          if (!headers.getHeaderNames().contains(key)) {

            // Decode header value as per the spec. Is there a faster way
            // to do this?
            value = value.replace(OCTET_92_92, OCTET_92)
                .replace(OCTET_92_99, OCTET_58)
                .replace(OCTET_92_110, OCTET_10)
                .replace(OCTET_92_114, OCTET_13);

            headers.put(key, value);
          }
        }
      }
    }

    return eoh;
  }

  /**
   * Reads the command of the frame. The command will be set in the frame after
   * reading.
   *
   * @param frame the partial frame that has been read to this point
   *
   * @throws IOException if there is an error reading from the underlying stream
   */
  private boolean readCommand(ByteBuf in) {

    boolean eoc = false;
    String line;

    do {
      line = readLine(in);
    } while (line != null && line.isEmpty());

    if (line != null) {
      command = Command.valueOf(line);
      eoc = true;
    }

    return eoc;
  }

  /**
   * The various frame parsing states when decoding a STOMP frame.
   */
  enum DecoderState {

    READ_COMMAND,
    READ_HEADERS,
    READ_BODY;
  }

}
