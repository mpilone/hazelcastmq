package org.mpilone.hazelcastmq.core;

/**
 * A converter responsible for converting a HazelcastMQ message into a wire
 * format to be sent via a Hazelcast queue or topic. It is assumed that the same
 * converter * implementation will be used on both the producing and consuming side to
 * ensure that the bytes can be properly converted back into a message.
 * 
 * @author mpilone
 */
public interface MessageConverter {

  /**
   * Converts the given message to an object to be added to a queue or topic.
    * 
   * @param message
   *          the message to convert
   * @return the converted message
   * @throws HazelcastMQException if there is an error converting the message
   */
   Object fromMessage(HazelcastMQMessage message)
      throws HazelcastMQException;

  /**
   * Converts the given object from a queue or topic into a message.
    * 
   * @param data
   *          the data to convert
   * @return the new message
   * @throws HazelcastMQException if there is an error converting the message
   */
   HazelcastMQMessage toMessage(Object data) throws HazelcastMQException;

}
