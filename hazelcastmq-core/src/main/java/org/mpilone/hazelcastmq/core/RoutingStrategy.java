package org.mpilone.hazelcastmq.core;

import java.util.Collection;

/**
 *
 * @author mpilone
 */
public interface RoutingStrategy {
  Collection<DataStructureKey> apply(Message<?> msg,
      Collection<DataStructureKey> targetKeys);
}
