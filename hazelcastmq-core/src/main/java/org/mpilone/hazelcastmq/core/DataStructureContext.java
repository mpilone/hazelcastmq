package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.BaseQueue;

/**
 * A context for locating data structures which may or may not be part of a
 * transaction. If {@code joinTransaction} is true and there is an active
 * transaction available to the resolver (either local or managed), the data
 * structure will be transactional. If false or if there is no transaction, the
 * data structure will be non-transactional. Not all data structures support
 * transactions and therefore the join flag may be ignored.
 *
 * @author mpilone
 */
public interface DataStructureContext {

  /**
   * Returns the queue with the given name.
   *
   * @param <E> the type of the entries in the queue
   * @param name the of the queue to lookup
   * @param joinTransaction true to get a transactional queue if possible, false
   * to always get a non-transactional queue
   *
   * @return the data structure instance
   */
  <E> BaseQueue<E> getQueue(String name, boolean joinTransaction);

  /**
   * Returns the map with the given name.
   *
   * @param <K> the type of the keys in the map
   * @param <V> the type of the values in the map
   * @param name the of the map to lookup
   * @param joinTransaction true to get a transactional map if possible, false
   * to always get a non-transactional map
   *
   * @return the data structure instance
   */
  <K, V> BaseMap<K, V> getMap(String name, boolean joinTransaction);

}
