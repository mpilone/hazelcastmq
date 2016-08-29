package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
interface InflightContext {

  void inflight(DataStructureKey channelKey, Message<?> message);
}
