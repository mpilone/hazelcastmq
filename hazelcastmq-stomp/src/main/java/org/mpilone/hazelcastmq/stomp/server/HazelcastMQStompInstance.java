package org.mpilone.hazelcastmq.stomp.server;

import org.mpilone.hazelcastmq.core.HazelcastMQ;

/**
 * A STOMP server backed by {@link HazelcastMQ}. The server is started
 * automatically at construction and will terminate when the {@link #shutdown()}
 * method is called.
  * 
 * @author mpilone
 */
public interface HazelcastMQStompInstance {

  /**
   * Shuts down the server socket. This method will block until the server is
   * shutdown completely.
   */
  void shutdown();

}
