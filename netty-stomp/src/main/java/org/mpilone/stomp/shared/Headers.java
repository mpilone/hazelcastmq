package org.mpilone.stomp.shared;

import java.util.Set;

public interface Headers {
  public static final String CONTENT_TYPE = "content-type";
  public static final String CONTENT_LENGTH = "content-length";
  public static final String DESTINATION = "destination";
  public static final String RECEIPT = "receipt";
  public static final String RECEIPT_ID = "receipt-id";
  public static final String ID = "id";
  public static final String ACK = "ack";
  public static final String TRANSACTION = "transaction";
  public static final String VERSION = "version";
  public static final String ACCEPT_VERSION = "accept-version";
  public static final String HOST = "host";
  public static final String LOGIN = "login";
  public static final String PASSCODE = "passcode";
  public static final String HEART_BEAT = "heart-beat";
  public static final String SESSION = "session";
  public static final String SERVER = "server";
  public static final String MESSAGE_ID = "message-id";
  public static final String SUBSCRIPTION = "subscription";
  public static final String MESSAGE = "message";

  public String get(String headerName);

  public Set<String> getHeaderNames();

  public String put(String headerName, String headerValue);

  public void remove(String headerName);
}
