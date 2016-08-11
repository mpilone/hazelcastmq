
package org.mpilone.hazelcastmq.core;

/**
 * Performs a "no-op" message conversion by simply returning the original
 * {@link Message}. This offers the best performance by simply
 * delegating to Hazelcast's serialization mechanism but it can make debugging
 * on the wire a little more difficult.
 *
 * @author mpilone
 */
public class NoOpMessageConverter implements MessageConverter {

  @Override
  public Object fromMessage(Message<?> message) throws
      HazelcastMQException {
    return message;
  }

  @Override
  public Message<?> toMessage(Object data) throws HazelcastMQException {
    return (Message<?>) data;
  }

}
