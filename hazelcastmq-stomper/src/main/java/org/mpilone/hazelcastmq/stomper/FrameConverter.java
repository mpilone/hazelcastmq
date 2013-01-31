package org.mpilone.hazelcastmq.stomper;

import javax.jms.*;

public interface FrameConverter {
  public Message fromFrame(Frame frame, Session session) throws JMSException;

  public Frame toFrame(Message msg) throws JMSException;

  public Destination fromFrameDestination(String destName, Session session)
      throws JMSException;

  public String toFrameDestination(Destination destination) throws JMSException;
}
