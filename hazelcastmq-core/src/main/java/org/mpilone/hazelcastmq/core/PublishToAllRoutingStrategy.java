
package org.mpilone.hazelcastmq.core;

import java.util.Collection;

/**
 *
 * @author mpilone
 */
public class PublishToAllRoutingStrategy implements RoutingStrategy {

  @Override
  public Collection<DataStructureKey> apply(
      Message<?> msg, Collection<DataStructureKey> targetKeys) {
    // TODO: Implement method
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
