package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public interface ReadReadyListener {
  void readReady(Channel channel);
}
