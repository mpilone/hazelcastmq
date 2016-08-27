package org.mpilone.hazelcastmq.core;

/**
 * A parent class that tracks its children (for one reason or another). The
 * child can tell the parent to remove it from track when it is closed or no
 * longer in use.
 *
 * @author mpilone
 * @param <C> the type of the child being tracked by the parent
 */
interface TrackingParent<C> {

  /**
   * Removes the child from the parent when the child is closed or no longer in
   * use. Normally the child calls this method when it is closed or destroyed.
   *
   * @param child the child to remove
   */
  void remove(C child);

}
