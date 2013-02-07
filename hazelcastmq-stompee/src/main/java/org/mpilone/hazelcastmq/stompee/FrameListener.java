package org.mpilone.hazelcastmq.stompee;

import org.mpilone.hazelcastmq.stomp.Frame;

public interface FrameListener {
  public void frameReceived(Frame frame);
}
