package org.mpilone.hazelcastmq;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;

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
   * @return the message as bytes
   * @throws IOException
   * @throws JMSException
   */
  public byte[] fromMessage(Message message) throws IOException, JMSException;

  /**
   * Converts the given block of bytes into a JMS message.
   * 
   * @param data
   *          the data to convert
   * @return the bytes as a message
   * @throws IOException
   * @throws JMSException
   */
  public Message toMessage(byte[] data) throws IOException, JMSException;

}
