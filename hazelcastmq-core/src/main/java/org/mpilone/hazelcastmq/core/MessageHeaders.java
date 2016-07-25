package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author mpilone
 */
public interface MessageHeaders extends Map<String, Object>, Serializable {

  static final String MESSAGE_ID = "id";

//   static final String CHANNEL = "channel";
  static final String REPLY_TO = "reply-to";

  static final String CORRELATION_ID = "correlation-id";

  static final String EXPIRATION = "expiration";

  static final String TIMESTAMP = "timestamp";

}
