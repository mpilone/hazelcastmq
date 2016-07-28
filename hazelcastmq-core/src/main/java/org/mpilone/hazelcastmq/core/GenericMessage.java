
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author mpilone
 */
public class GenericMessage<T> implements Message<T>, Serializable {

  private final MessageHeaders headers;
  private T payload;

  public GenericMessage(T payload) {
    this(payload, new MessageHeaders(null));
  }

  public GenericMessage(T payload, Map<String, Object> headers) {
    this.payload = payload;
    this.headers = new MessageHeaders(headers);
  }

  public GenericMessage(T payload, MessageHeaders headers) {
    this.payload = payload;
    this.headers = headers;
  }

  @Override
  public MessageHeaders getHeaders() {
    return headers;
  }

  @Override
  public T getPayload() {
    return payload;
  }

}
