package org.mpilone.hazelcastmq.core;

/**
 * A converter responsible for converting a JMS message into a wire format to be
 * sent via the HazelcastMQ. It is assumed that the same converter
 * implementation will be used on both the producing and consuming side to
 * ensure that the bytes can be properly converted back into a message.
 * 
 * @author mpilone
 */
public interface MessageConverter {

  /**
   * Converts the given JMS message to a block of bytes.
   * 
   * @param message
   *          the message to convert
   * @return the converted message
   * @throws HazelcastMQException if there is an error converting the message
   */
   Object fromMessage(HazelcastMQMessage message)
      throws HazelcastMQException;

  /**
   * Converts the given block of bytes into a JMS message.
   * 
   * @param data
   *          the data to convert
   * @return the bytes as a message
   * @throws HazelcastMQException if there is an error converting the message
   */
   HazelcastMQMessage toMessage(Object data) throws HazelcastMQException;

}
