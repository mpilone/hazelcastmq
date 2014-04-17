
package org.mpilone.hazelcastmq.core;

/**
 * Performs a "no-op" message conversion by simply returning the original
 * {@link HazelcastMQMessage}. This offers the best performance by simply
 * delegating to Hazelcast's serialization mechanism but it can make debugging
 * on the wire a little more difficult.
 *
 * @author mpilone
 */
public class NoOpMessageConverter implements MessageConverter {

  @Override
  public Object fromMessage(HazelcastMQMessage message) throws
      HazelcastMQException {
    return message;
  }

  @Override
  public HazelcastMQMessage toMessage(Object data) throws HazelcastMQException {
    return (HazelcastMQMessage) data;
  }

}
