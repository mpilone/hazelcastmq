package org.mpilone.yeti;

import java.util.Collection;
import java.util.Map;

/**
 * The frame headers which are simple String/String pairs. Constants are defined
 * for the standard STOMP headers.
 *
 * @author mpilone
 */
public interface Headers {
   static final String CONTENT_TYPE = "content-type";
   static final String CONTENT_LENGTH = "content-length";
   static final String DESTINATION = "destination";
   static final String RECEIPT = "receipt";
   static final String RECEIPT_ID = "receipt-id";
   static final String ID = "id";
   static final String ACK = "ack";
   static final String TRANSACTION = "transaction";
   static final String VERSION = "version";
   static final String ACCEPT_VERSION = "accept-version";
   static final String HOST = "host";
   static final String LOGIN = "login";
   static final String PASSCODE = "passcode";
   static final String HEART_BEAT = "heart-beat";
   static final String SESSION = "session";
   static final String SERVER = "server";
   static final String MESSAGE_ID = "message-id";
   static final String SUBSCRIPTION = "subscription";
   static final String MESSAGE = "message";

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
}
