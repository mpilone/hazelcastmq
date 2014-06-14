
package org.mpilone.yeti;

import static java.lang.String.format;

import java.util.*;

/**
 * STOMP protocol versions.
 *
 * @author mpilone
 */
public enum StompVersion {

  VERSION_1_0("1.0"),
  VERSION_1_1("1.1"),
  VERSION_1_2("1.2");

  private final String headerValue;
  private static final Map<String, StompVersion> HEADER_VALUE_LOOKUP_MAP;

  static {
    Map<String, StompVersion> map = new HashMap<>();
    for (StompVersion v : StompVersion.values()) {
      map.put(v.getHeaderValue(), v);
    }
    HEADER_VALUE_LOOKUP_MAP = Collections.unmodifiableMap(map);
  }

  /**
   * Constructs the version with the given header value String.
   *
   * @param headerValue the header value String such as "1.2"
   */
  private StompVersion(String headerValue) {
    this.headerValue = headerValue;
  }

  /**
   * Returns the version as a value that can be used in a STOMP frame header.
   * For example, "1.2" or "1.1".
   *
   * @return the version has a header value string
   */
  public String getHeaderValue() {
    return headerValue;
  }

  /**
   * Sorts the values of the enumeration sorted from the highest (and most
   * desirable) protocol version to the lowest version.
   *
   * @param versions the versions to sort
   */
  public static void sortDescending(StompVersion[] versions) {

    Arrays.sort(versions, new Comparator<StompVersion>() {
      @Override
      public int compare(StompVersion o1, StompVersion o2) {
        return -1 * (o1.getHeaderValue().compareTo(o2.getHeaderValue()));
      }
    });
  }

  /**
   * Returns the version matching the given header value.
   *
   * @param headerValue the header value such as "1.2"
   *
   * @return the version
   * @throws IllegalArgumentException if the header values does not match any
   * known version
   */
  public static StompVersion fromHeaderValue(String headerValue) throws
      IllegalArgumentException {
    StompVersion v = HEADER_VALUE_LOOKUP_MAP.get(headerValue);
    if (v == null) {
      throw new IllegalArgumentException(format(
          "Unknown version header value %s.", headerValue));
    }
    else {
      return v;
    }
  }

}
