package org.mpilone.hazelcastmq.core;

import java.util.Set;

public interface Headers {
  public static final String CONTENT_TYPE = "content-type";

  public static final String CONTENT_LENGTH = "content-length";

  public static final String MESSAGE_ID = "id";

  public static final String DESTINATION = "destination";

  public static final String REPLY_TO = "reply-to";

  public static final String CORRELATION_ID = "correlation-id";

  public static final String EXPIRATION = "expiration";

  public static final String DESTINATION_QUEUE_PREFIX = "/queue/";

  public static final String DESTINATION_TEMPORARY_QUEUE_PREFIX = "/temp-queue/";

  public static final String DESTINATION_TOPIC_PREFIX = "/topic/";

  public static final String DESTINATION_TEMPORARY_TOPIC_PREFIX = "/temp-topic/";

  public String get(String headerName);

  public Set<String> getHeaderNames();

  public String put(String headerName, String headerValue);

  public void remove(String headerName);
}
