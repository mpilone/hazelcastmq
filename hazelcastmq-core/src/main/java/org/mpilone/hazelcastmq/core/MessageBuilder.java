
package org.mpilone.hazelcastmq.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for quickly building {@link Message} instances or copying
 * existing messages.
 *
 * @author mpilone
 * @param <P> the type of the message payload
 */
public class MessageBuilder<P> {

  private final P payload;
  private final Map<String, Object> headers = new HashMap<>();

  private MessageBuilder(P payload) {
    this.payload = payload;
  }

  public Message<P> build() {
    return new GenericMessage<>(payload, headers);
  }

  public MessageBuilder<P> copyHeaders(Map<String, Object> headers) {
    if (headers != null) {
      this.headers.putAll(headers);
    }
    return this;
  }

  public static <P> Message<P> createMessage(P payload, MessageHeaders headers) {
    return MessageBuilder.withPayload(payload).copyHeaders(headers).build();
  }

  public static <P> MessageBuilder<P> fromMessage(Message<P> message) {
    return MessageBuilder.withPayload(message.getPayload()).copyHeaders(message.
        getHeaders());
  }

  public MessageBuilder<P> setHeader(String headerName, Object headerValue) {
    headers.put(headerName, headerValue);
    return this;
  }

  public MessageBuilder<P> removeHeader(String headerName) {
    headers.remove(headerName);
    return this;
  }

  public MessageBuilder<P> removeHeaders(String... headerNames) {
    for (String headerName : headerNames) {
      headers.remove(headerName);
    }
    return this;
  }

  public MessageBuilder<P> setHeaderIfAbsent(String headerName,
      Object headerValue) {
    if (!headers.containsKey(headerName)) {
      headers.put(headerName, headerValue);
    }

    return this;
  }

  public static <P> MessageBuilder<P> withPayload(P payload) {
    return new MessageBuilder<>(payload);
  }

}
