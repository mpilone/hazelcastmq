
package org.mpilone.yeti;

import static org.junit.Assert.*;
import static org.mpilone.yeti.StompConstants.*;

import org.junit.Test;

import io.netty.buffer.*;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Test case for the {@link StompFrameDecoder}.
 *
 * @author mpilone
 */
public class StompFrameDecoderTestCase {

  /**
   * Tests decoding a frame that is too long because of the header length.
   */
  @Test
  public void testDecodeFrame_LongHeaders() {

    int maxFrameSize = 2 * 1024;

    String header1 = "header1:";
    for (int i = 0; i < maxFrameSize / 2; ++i) {
      header1 += 'a';
    }
    String header2 = "header2:";
    for (int i = 0; i < maxFrameSize / 2; ++i) {
      header1 += 'b';
    }

    EmbeddedChannel ec =
        new EmbeddedChannel(new StompFrameDecoder(maxFrameSize));

    final byte[] body = "This is the body.".getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(header1.getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(header2.getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST));
  }

  /**
   * Tests decoding a frame that is too long because of the body length.
   */
  @Test
  public void testDecodeFrame_LongBody() {

    int maxFrameSize = 2 * 1024;

    String body = "";
    for (int i = 0; i < maxFrameSize; ++i) {
      body += 'a';
    }

    EmbeddedChannel ec =
        new EmbeddedChannel(new StompFrameDecoder(maxFrameSize));

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body.getBytes(UTF_8));
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST));
  }

  /**
   * Tests decoding a frame that is too long because of the body length
   * indicated in the content length.
   */
  @Test
  public void testDecodeFrame_LongContentLength() {

    int maxFrameSize = 2 * 1024;

    String contentLengthHeader = Headers.CONTENT_LENGTH + ":" + maxFrameSize;

    String body = "";
    for (int i = 0; i < maxFrameSize; ++i) {
      body += 'a';
    }

    EmbeddedChannel ec =
        new EmbeddedChannel(new StompFrameDecoder(maxFrameSize));

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(contentLengthHeader.getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body.getBytes(UTF_8));
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST));
  }

  /**
   * Tests decoding a single, simple frame.
   */
  @Test
  public void testDecodeFrame() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = "This is the body.".getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertEquals(Command.CONNECT, actualFrame.getCommand());
    assertEquals(1, actualFrame.getHeaders().getHeaderNames().size());
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains("header1"));
    assertArrayEquals(body, actualFrame.getBody());
  }

  /**
   * Tests decoding a frame when the body length is specified via the content
   * length header.
   */
  @Test
  public void testDecodeFrame_ContentLengthBody() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = ("This is " + NULL_CHAR + " the body.").getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes((Headers.CONTENT_LENGTH + ":" + body.length).getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertEquals(Command.CONNECT, actualFrame.getCommand());
    assertEquals(2, actualFrame.getHeaders().getHeaderNames().size());
    assertArrayEquals(body, actualFrame.getBody());
  }

  /**
   * Tests decoding a frame when the body length is specified via the content
   * length header but the size is wrong and the NULL_CHAR isn't found when
   * expected.
   */
  @Test
  public void testDecodeFrame_ContentLengthBody_WrongSize() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = ("This is the body.").getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes((Headers.CONTENT_LENGTH + ":" + (body.length - 2)).getBytes(
        UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST));
    assertNotNull(actualFrame.getHeaders().get(
        StompFrameDecoder.HEADER_BAD_REQUEST));
  }

  /**
   * Tests decoding a frame that has a lot of leading EOL characters.
   */
  @Test
  public void testDecodeFrame_LeadingEOLs() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = "This is the body.".getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(CARRIAGE_RETURN_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(CARRIAGE_RETURN_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertEquals(Command.CONNECT, actualFrame.getCommand());
  }
  /**
   * Tests decoding a frame that has a lot of trailing EOL characters.
   */
  @Test
  public void testDecodeFrame_TrailingEOLs() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = "This is the body.".getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();

    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(CARRIAGE_RETURN_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(CARRIAGE_RETURN_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertEquals(Command.CONNECT, actualFrame.getCommand());
  }

  /**
   * Tests decoding a frame when half the header arrives in one write and it is
   * completed in a second write.
   */
  @Test
  public void testDecodeFrame_SplitHeader() {

    EmbeddedChannel ec = new EmbeddedChannel(new StompFrameDecoder());

    final byte[] body = "This is the body.".getBytes(UTF_8);

    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(Command.CONNECT.name().getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header1:value1".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header2:val".getBytes(UTF_8));
    ec.writeInbound(buf);

    buf = Unpooled.buffer();
    buf.writeBytes("ue2".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes("header3:value3".getBytes(UTF_8));
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeByte(LINE_FEED_CHAR);
    buf.writeBytes(body);
    buf.writeByte(NULL_CHAR);
    ec.writeInbound(buf);

    Object actual = ec.readInbound();
    assertNotNull(actual);
    assertTrue(actual instanceof Frame);

    Frame actualFrame = (Frame) actual;
    assertEquals(Command.CONNECT, actualFrame.getCommand());
    assertEquals(3, actualFrame.getHeaders().getHeaderNames().size());
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains("header1"));
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains("header2"));
    assertTrue(actualFrame.getHeaders().getHeaderNames().contains("header3"));
  }

}
