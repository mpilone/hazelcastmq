package org.mpilone.hazelcastmq.camel;


import org.apache.camel.Message;
import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

/**
 * A converter responsible for converting a Camel message into a
 * {@link HazelcastMQMessage}.
 * 
 * @author mpilone
 */
public interface MessageConverter {

  /**
   * Converts the given Camel {@link Message} to a
   * {@link org.mpilone.hazelcastmq.core.HazelcastMQMessage}.
   * 
   * @param message
   *          the message to convert
   * @return the MQ message
   */
  public HazelcastMQMessage fromCamelMessage(Message message);

  /**
   * Converts the given MQ message into a Camel {@link Message}.
    * 
   * @param mqMsg
   *          the MQ message to convert
   * @return the Camel message
   */
  public Message toCamelMessage(HazelcastMQMessage mqMsg);

}
