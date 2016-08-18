package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.BaseQueue;

/**
 *
 * @author mpilone
 */
interface DataStructureResolver {

  /**
   * Returns the queue with the given name. If {@code joinTransaction} is true
   * and there is an active transaction available to the resolver (either local
   * or managed), the queue will be transactional. If false or if there is no
   * transaction, the queue will be non-transactional.
   *
   * @param <E> the type of the elements in the queue
   * @param name the name of the distributed queue
   * @param joinTransaction true to get a transactional queue if possible, *
   * false to always get a non-transactional queue
   *
   * @return the queue instance
   */
  <E> BaseQueue<E> getQueue(String name, boolean joinTransaction);

  /**
   * Returns the map with the given name. If {@code joinTransaction} is true and
   * there is an active transaction available to the resolver (either local or
   * managed), the map will be transactional. If false or if there is no
   * transaction, the map will be non-transactional.
   *
   * @param <K> the type of the keys in the map
   * @param <V> the type of the values in the map
   * @param name the name of the distributed map
   * @param joinTransaction true to get a transactional map if possible, false
   * to always get a non-transactional map
   *
   * @return the queue instance
   */
  <K, V> BaseMap<K, V> getMap(String name, boolean joinTransaction);

}
