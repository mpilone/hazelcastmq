package org.mpilone.hazelcastmq.stomp;

import org.mpilone.hazelcastmq.core.Broker;
import org.mpilone.hazelcastmq.core.HazelcastMQ;

/**
 * An adapter that implements the STOMP protocol and adapts and delegates all
 * operations to a {@link HazelcastMQ} {@link Broker}. The server is started
 * automatically at construction and will terminate when the {@link #close()}
 * method is called.
  * 
 * @author mpilone
 */
public interface StompAdapter extends AutoCloseable {

  /**
   * Closes the server socket. This method will block until the server is
   * shutdown completely.
   */
  @Override
  void close();

}
