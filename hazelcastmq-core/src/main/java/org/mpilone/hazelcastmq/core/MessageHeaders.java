package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.*;

/**
 * The headers on a message. The headers are immutable once constructed.
 *
 * @author mpilone
 */
public class MessageHeaders implements Map<String, Object>, Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ID_VALUE_NONE = new UUID(0, 0).toString();

  /**
   * The unique ID of the message. The value type is {@link String}.
   */
  public static final String ID = "id";

  /**
   * The reply-to address for request/reply type messaging. The value type is
   * {@link DataStructureKey}.
   */
  public static final String REPLY_TO = "replyTo";

  /**
   * The correlation ID for request/reply type messaging. The value type is
   * {@link String}.
   */
  public static final String CORRELATION_ID = "correlationId";

  /**
   * The expiration time for the message. The value type is {@link Long}
   * containing milliseconds since the epoch.
   */
  public static final String EXPIRATION = "expiration";

  /**
   * The time the message was created/sent. The value type is {@link Long}
   * containing milliseconds since the epoch.
   */
  public static final String TIMESTAMP = "timestamp";

  private final Map<String, Object> headers;

  /**
   * Constructs the headers by copying the values in the given map. The ID and
   * TIMESTAMP headers will be automatically generated.
   *
   * @param headers the headers to copy.
   */
  public MessageHeaders(Map<String, Object> headers) {
    this(headers, null, null);
  }

  /**
   * Constructs the headers by copying the values in the given map. The ID and
   * TIMESTAMP can be specified or they will be generated if null.
   *
   * @param headers the message headers
   * @param id the ID value or null to generate it
   * @param timestamp the TIMESTAMP value or null to generate it
   */
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

  /**
   * Returns the message ID.
   *
   * @return the message ID
   * @see #ID
   */
  public String getId() {
    return get(ID, String.class);
  }

  /**
   * Returns the message timestamp.
   *
   * @return the message timestamp
   * @see #TIMESTAMP
   */
  public Long getTimestamp() {
    return get(TIMESTAMP, Long.class);
  }

  /**
   * Returns the reply-to channel key.
   *
   * @return the reply-to channel key
   * @see #REPLY_TO
   */
  public DataStructureKey getReplyTo() {
    return get(REPLY_TO, DataStructureKey.class);
  }

  /**
   * Returns the request/reply correlation ID.
   *
   * @return the correlation ID
   * @see #CORRELATION_ID
   */
  public String getCorrelationId() {
    return get(CORRELATION_ID, String.class);
  }

  /**
   * Returns the raw headers that can be modified in a subclass. Message headers
   * should be immutable so in most cases the headers should only be modified in
   * the constructor of a subclass.
   *
   * @return the raw headers
   */
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

  /**
   * Throws an {@link UnsupportedOperationException} because headers are
   * immutable.
   *
   * @param key not used
   * @param value not used
   *
   * @return not used
   */
  @Override
  public Object put(String key, Object value) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  /**
   * Throws an {@link UnsupportedOperationException} because headers are
   * immutable.
   *
   * @param key not used
   *
   * @return not used
   */
  @Override
  public Object remove(Object key) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  /**
   * Throws an {@link UnsupportedOperationException} because headers are
   * immutable.
   *
   * @param m not used
   */
  @Override
  public void putAll(
      Map<? extends String, ? extends Object> m) {
    throw new UnsupportedOperationException("MessageHeaders is immutable.");
  }

  /**
   * Throws an {@link UnsupportedOperationException} because headers are
   * immutable.
   */
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
