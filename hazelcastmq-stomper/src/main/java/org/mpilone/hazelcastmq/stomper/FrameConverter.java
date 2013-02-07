package org.mpilone.hazelcastmq.stomper;

import javax.jms.*;

import org.mpilone.hazelcastmq.stomp.Command;
import org.mpilone.hazelcastmq.stomp.Frame;

/**
 * Converts a STOMP frame into a JMS message and vice versa. All mappings of
 * frames to message types and headers is handled by the converter.
 * 
 * @author mpilone
 */
public interface FrameConverter {
  /**
   * Converts the given STOMP frame into a JMS message. The message will be
   * constructed from the given session.
   * 
   * @param frame
   *          the frame to convert
   * @param session
   *          the session to use for message creation
   * @return the new JMS message
   * @throws JMSException
   *           if there is an error creating a message
   */
  public Message fromFrame(Frame frame, Session session) throws JMSException;

  /**
   * Converts the given JMS message to a STOMP frame. The frame will default to
   * a {@link Command#MESSAGE} command but it may be changed later.
   * 
   * @param msg
   *          the JMS message to convert
   * @return the new stomp frame
   * @throws JMSException
   *           if there is an error reading from the message
   */
  public Frame toFrame(Message msg) throws JMSException;

  /**
   * Converts the frame header destination name into a JMS destination using the
   * given session.
   * 
   * @param destName
   *          the destination name as it appeared in a frame header
   * @param session
   *          the session to use for destination creation/lookup
   * @return the JMS destination
   * @throws JMSException
   *           if there is an error creating the destination
   */
  public Destination fromFrameDestination(String destName, Session session)
      throws JMSException;

  /**
   * Converts the JMS destination to a destination name appropriate for a frame
   * header.
   * 
   * @param destination
   *          the JMS destination to convert
   * @return the String representation of the destination
   * @throws JMSException
   *           if there is an error reading from the destination
   */
  public String toFrameDestination(Destination destination) throws JMSException;
}
