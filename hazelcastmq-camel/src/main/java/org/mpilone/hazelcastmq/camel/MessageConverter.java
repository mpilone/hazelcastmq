package org.mpilone.hazelcastmq.camel;


import org.apache.camel.Message;

/**
 * A converter responsible for converting a Camel message into a
 * {@link org.mpilone.hazelcastmq.core.Message}.
  * 
 * @author mpilone
 */
public interface MessageConverter {

  /**
   * Converts the given Camel {@link Message} to a MQ
   * {@link org.mpilone.hazelcastmq.core.Message}.
   * 
   * @param message
   *          the message to convert
   * @return the MQ message
   */
  public org.mpilone.hazelcastmq.core.Message<?> fromCamelMessage(
      Message message);

  /**
   * Converts the given MQ {@link org.mpilone.hazelcastmq.core.Message} into a
   * Camel {@link Message}.
    * 
   * @param mqMsg
   *          the MQ message to convert
   * @return the Camel message
   */
  public Message toCamelMessage(org.mpilone.hazelcastmq.core.Message<?> mqMsg);

}
