package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 * @param <C> the type of the child being tracked by the parent
 */
interface TrackingParent<C> {

  void remove(C child);

}
