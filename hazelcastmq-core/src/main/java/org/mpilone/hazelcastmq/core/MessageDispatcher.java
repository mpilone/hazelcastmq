
package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public interface MessageDispatcher {

  void start();

  void stop();

  @FunctionalInterface
  interface Performer {

    void perform(Message<?> msg);
  }

}
