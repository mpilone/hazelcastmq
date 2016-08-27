
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.Map;

/**
 * A generic message implementation that takes a body and headers.
 *
 * @author mpilone
 * @param <P> the type of the message payload
 */
public class GenericMessage<P> implements Message<P>, Serializable {

  private final MessageHeaders headers;
  private P payload;

  public GenericMessage(P payload) {
    this(payload, new MessageHeaders(null));
  }

  public GenericMessage(P payload, Map<String, Object> headers) {
    this.payload = payload;
    this.headers = new MessageHeaders(headers);
  }

  public GenericMessage(P payload, MessageHeaders headers) {
    this.payload = payload;
    this.headers = headers;
  }

  @Override
  public MessageHeaders getHeaders() {
    return headers;
  }

  @Override
  public P getPayload() {
    return payload;
  }

}
