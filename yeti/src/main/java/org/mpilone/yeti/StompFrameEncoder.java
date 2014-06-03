package org.mpilone.yeti;

import static org.mpilone.yeti.StompConstants.*;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 *
 * @author mpilone
 */
public class StompFrameEncoder extends MessageToByteEncoder<Frame> {

  @Override
  protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out)
      throws Exception {

    // Some sanity checks before we serialize the frame.
    if (frame == null || frame.getCommand() == null) {
      throw new IllegalArgumentException(
          "A frame command is required for all frames.");
    }

    encodeFrame(frame, out, true);
  }

  /**
   * Encodes the given frame into the byte buffer.
   *
   * @param frame the frame to encode
   * @param out the output byte buffer
   * @param nullTerminate true to include null termination in the byte buffer,
   * false to not include it
   */
  public static void encodeFrame(Frame frame, ByteBuf out, boolean nullTerminate) {
    // Write the command
    out.writeBytes(frame.getCommand().name().getBytes(UTF_8));
    out.writeByte(LINE_FEED_CHAR);

    // log.debug("Wrote command: " + frame.getCommand());
    // Write the headers
    for (String key : frame.getHeaders().getHeaderNames()) {
      String value = frame.getHeaders().get(key);

      // Encode header value as per the specification.
      value = value.replace(OCTET_92, OCTET_92_92);
      value = value.replace(OCTET_58, OCTET_92_99);
      value = value.replace(OCTET_10, OCTET_92_110);
      value = value.replace(OCTET_13, OCTET_92_114);

      out.writeBytes(key.getBytes(UTF_8));
      out.writeByte(COLON_CHAR);
      out.writeBytes(value.getBytes(UTF_8));
      out.writeByte(LINE_FEED_CHAR);
    }
    // log.debug("Wrote headers: " + frame.getHeaders());

    // If we have a body and we don't have a content-length header, write one.
    if (frame.getBody() != null
        && !frame.getHeaders().getHeaderNames().contains(Headers.CONTENT_LENGTH)) {
      out.writeBytes(Headers.CONTENT_LENGTH.getBytes(UTF_8));
      out.writeByte(COLON_CHAR);
      out.writeInt(frame.getBody().length);
      out.writeByte(LINE_FEED_CHAR);
    }

    // Blank line to separate headers from the body.
    out.writeByte(LINE_FEED_CHAR);

    // Write the body.
    if (frame.getBody() != null) {
      out.writeBytes(frame.getBody());
    }
    // log.debug("Wrote body: " + frame.getBody());

    if (nullTerminate) {
      // Finally the terminator.
      out.writeByte(NULL_CHAR);
    }
  }

}
