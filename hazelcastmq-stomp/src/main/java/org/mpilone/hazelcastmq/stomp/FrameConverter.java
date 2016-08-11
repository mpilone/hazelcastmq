package org.mpilone.hazelcastmq.stomp;

import org.mpilone.hazelcastmq.core.Message;
import org.mpilone.yeti.Command;
import org.mpilone.yeti.Frame;

/**
 * Converts a STOMP frame into a {@link Message} and vice versa. All
 * mappings of frames to message types and headers is handled by the converter.
 * 
 * @author mpilone
 */
public interface FrameConverter {
  /**
   * Converts the given STOMP frame into a {@link Message}. The
   * message will be constructed from the given session.
   * 
   * @param frame
   *          the frame to convert
   * @return the new HazelcastMQ message
   */
  public Message<?> fromFrame(Frame frame);

  /**
   * Converts the given {@link Message} to a STOMP frame. The frame
   * will default to a {@link Command#MESSAGE} command but it may be changed
   * later.
   * 
   * @param msg
   *          the HazelcastMQ message to convert
   * @return the new STOMP frame
   */
  public Frame toFrame(Message<?> msg);
}
