package org.mpilone.stomp.shared;

import java.nio.charset.Charset;

/**
 * Constants for use in the STOMP protocol. Constants are provided for header
 * encoding:
 * <ul>
 * <li>\r (octet 92 and 114) translates to carriage return (octet 13)</li>
 * <li>\n (octet 92 and 110) translates to line feed (octet 10)</li>
 * <li>\c (octet 92 and 99) translates to : (octet 58)</li>
 * <li>\\ (octet 92 and 92) translates to \ (octet 92)</li>
 * </ul>
 * 
 * @author mpilone
 */
public interface StompConstants {

  /**
   * The UTF-8 character set used for all conversions.
   */
  public final static Charset UTF_8 = Charset.forName("UTF-8");
  /**
   * The null terminator that must appear after each STOMP frame.
   */
  public static final char NULL_CHAR = '\0';
  public static final char COLON_CHAR = ':';
  public static final char LINE_FEED_CHAR = '\n';
  public static final char CARRIAGE_RETURN_CHAR = '\r';

  public static final String OCTET_92_92 = "" + (char) 92 + (char) 92;
  public static final String OCTET_92_99 = "" + (char) 92 + (char) 99;
  public static final String OCTET_92_110 = "" + (char) 92 + (char) 110;
  public static final String OCTET_92_114 = "" + (char) 92 + (char) 114;

  public static final String OCTET_13 = "" + (char) 13;
  public static final String OCTET_10 = "" + (char) 10;
  public static final String OCTET_58 = "" + (char) 58;
  public static final String OCTET_92 = "" + (char) 92;
}
