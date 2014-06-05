package org.mpilone.hazelcastmq.core;

import java.util.Collection;
import java.util.Map;

/**
 * The headers in a {@link HazelcastMQMessage}. The headers are simple key/value
 * pairs. Constants are provided for all the commonly used headers.
 *
 * @author mpilone
 */
public interface Headers {
   static final String CONTENT_TYPE = "content-type";

   static final String CONTENT_LENGTH = "content-length";

   static final String MESSAGE_ID = "id";

   static final String DESTINATION = "destination";

   static final String REPLY_TO = "reply-to";

   static final String CORRELATION_ID = "correlation-id";

   static final String EXPIRATION = "expiration";

   static final String DESTINATION_QUEUE_PREFIX = "/queue/";

   static final String DESTINATION_TEMPORARY_QUEUE_PREFIX = "/temp-queue/";

   static final String DESTINATION_TOPIC_PREFIX = "/topic/";

   static final String DESTINATION_TEMPORARY_TOPIC_PREFIX = "/temp-topic/";

  /**
   * Returns the value of the header with the given name or null if the header
   * is not defined.
   *
   * @param headerName the name of the header
   *
   * @return the value or null
   */
  String get(String headerName);

  /**
   * Returns the names of the headers defined in the frame.
   *
   * @return an unmodifiable collection of names
   */
  Collection<String> getHeaderNames();

  /**
   * Returns an unmodifiable {@link Map} of header names and values.
   *
   * @return an unmodifiable map
   */
  Map<String, String> getHeaderMap();

  /**
   * Sets the header with the given name and value. Any previous value of the
   * header will be replaced.
   *
   * @param headerName the header name
   * @param headerValue the header value
   *
   * @return the previous value of the header or null if it wasn't set
   */
   String put(String headerName, String headerValue);

  /**
   * Removes the header with the given name. If there is no matching header,
   * this method does nothing.
   *
   * @param headerName the name of the header to remove
   */
   void remove(String headerName);
}
