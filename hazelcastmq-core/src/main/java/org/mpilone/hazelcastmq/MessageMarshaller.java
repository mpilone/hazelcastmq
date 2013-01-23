package org.mpilone.hazelcastmq;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * A marshaller responsible for marshalling a JMS message into a wire format to
 * be sent via the HazelcastMQ. It is assumed that the same marshaller
 * implementation will be used on both the producing and consuming side to
 * ensure that the messages can be properly unmarshalled.
 * 
 * @author mpilone
 * 
 */
public interface MessageMarshaller {

  /**
   * Marshals the given JMS message to a block of bytes.
   * 
   * @param message
   *          the message to marshal
   * @return the marshalled message
   * @throws IOException
   * @throws JMSException
   */
  public byte[] marshal(Message message) throws IOException, JMSException;

  /**
   * Unmarshals the given block of bytes into a JMS message.
   * 
   * @param data
   *          the data to unmarshal
   * @return the unmarshalled message
   * @throws IOException
   * @throws JMSException
   */
  public Message unmarshal(byte[] data) throws IOException, JMSException;

}
