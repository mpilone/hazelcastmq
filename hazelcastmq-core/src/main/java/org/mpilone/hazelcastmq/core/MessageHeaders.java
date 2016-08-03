package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.*;

/**
 *
 * @author mpilone
 */
public class MessageHeaders implements Map<String, Object>, Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ID_VALUE_NONE = new UUID(0, 0).toString();

  public static final String ID = "id";

//   static final String CHANNEL = "channel";
  public static final String REPLY_TO = "replyTo";

  public static final String CORRELATION_ID = "correlationId";

  public static final String EXPIRATION = "expiration";

  public static final String TIMESTAMP = "timestamp";

  private final Map<String, Object> headers;

  public MessageHeaders(Map<String, Object> headers) {
    this(headers, null, null);
  }

  protected MessageHeaders(Map<String, Object> headers, String id,
      Long timestamp) {
    this.headers = (headers != null ? new HashMap<>(headers) :
        new HashMap<>());

    if (id == null) {
      this.headers.put(ID, UUID.randomUUID().toString());
    }
    else if (id.equals(ID_VALUE_NONE)) {
      this.headers.remove(ID);
    }
    else {
      this.headers.put(ID, id);
    }

    if (timestamp == null) {
      this.headers.put(TIMESTAMP, System.currentTimeMillis());
    }
    else if (timestamp < 0) {
      this.headers.remove(TIMESTAMP);
    }
    else {
      this.headers.put(TIMESTAMP, timestamp);
    }
  }

  public String getId() {
    return get(ID, String.class);
  }

  public Long getTimestamp() {
    return get(TIMESTAMP, Long.class);
  }

  public DataStructureKey getReplyTo() {
    return get(REPLY_TO, DataStructureKey.class);
  }

  public String getCorrelationId() {
    return get(CORRELATION_ID, String.class);
  }

  protected Map<String, Object> getRawHeaders() {
    return headers;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key, Class<T> type) {
    Object value = this.headers.get(key);
    if (value == null) {
      return null;
    }
    if (!type.isAssignableFrom(value.getClass())) {
      throw new IllegalArgumentException("Incorrect type specified for header '"
          + key + "'. Expected [" + type + "] but actual type is [" + value.
          getClass() + "]");
    }
    return (T) value;
  }

  @Override
  public int size() {
    return headers.size();
  }

  @Override
  public boolean isEmpty() {
    return headers.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return headers.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return headers.containsValue(value);
  }

  @Override
  public Object get(Object key) {
    return headers.get(key);
  }

  @Override
  public Object put(String key, Object value) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  @Override
  public Object remove(Object key) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  @Override
  public void putAll(
      Map<? extends String, ? extends Object> m) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  @Override
  public Set<String> keySet() {
    return Collections.unmodifiableSet(headers.keySet());
  }

  @Override
  public Collection<Object> values() {
    return Collections.unmodifiableCollection(headers.values());
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return Collections.unmodifiableMap(headers).entrySet();
  }

}
