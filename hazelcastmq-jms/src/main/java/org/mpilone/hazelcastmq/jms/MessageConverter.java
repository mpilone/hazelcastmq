package org.mpilone.hazelcastmq.jms;

import javax.jms.JMSException;
import javax.jms.Message;

import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

/**
 * A converter responsible for converting a JMS message into a
 * {@link HazelcastMQMessage}. It is assumed that the same converter
 * implementation will be used on both the producing and consuming side to
 * ensure that a {@link HazelcastMQMessage} can be properly converted back into
 * a message.
 * 
 * @author mpilone
 */
public interface MessageConverter {

  /**
   * Converts the given JMS {@link Message} to a
   * {@link org.mpilone.hazelcastmq.core.HazelcastMQMessage}.
   * 
   * @param message
   *          the message to convert
   * @return the MQ message
   * @throws JMSException if there is a problem converting the message
   */
  public HazelcastMQMessage fromJmsMessage(Message message) throws JMSException;

  /**
   * Converts the given MQ message into a JMS {@link Message}.
   * 
   * @param mqMsg
   *          the MQ message to convert
   * @return the JMS message
   * @throws JMSException if there is a problem converting the message
   */
  public Message toJmsMessage(HazelcastMQMessage mqMsg) throws JMSException;

}
