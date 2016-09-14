
package org.mpilone.hazelcastmq.core;

/**
 * Dispatches messages to a {@link Performer} as the messages become available.
 * How the dispatcher works is implementation dependant but the most common
 * implementation is to use a dedicated thread that performs a channel receive
 * and then dispatches any received messages.
 *
 * @author mpilone
 */
public interface MessageDispatcher {

  /**
   * Starts the dispatcher.
   */
  void start();

  /**
   * Stopps the dispatcher. This method blocks until the dispatcher is
   * completely stopped.
   */
  void stop();

  @FunctionalInterface
  interface Performer {

    void perform(Message<?> msg);
  }

}
