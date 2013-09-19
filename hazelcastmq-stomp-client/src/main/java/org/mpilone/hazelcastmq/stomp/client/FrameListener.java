package org.mpilone.hazelcastmq.stomp.client;

import org.mpilone.hazelcastmq.stomp.Frame;

/**
 * A listener that can be registered to receive frames as they arrive at the
 * Stompee client. The message will be pushed to the listener rather than using
 * polling like with the
 * {@link HazelcastMQStompClient#receive(long, java.util.concurrent.TimeUnit)}
 * method.
 * 
 * @author mpilone
 */
public interface FrameListener {

  /**
   * Called when a new frame arrives.
   * 
   * @param frame
   *          the frame to be processed
   */
  public void frameReceived(Frame frame);
}
