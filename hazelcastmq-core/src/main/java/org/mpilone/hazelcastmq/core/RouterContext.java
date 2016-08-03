package org.mpilone.hazelcastmq.core;

import java.util.Map;

/**
 *
 * @author mpilone
 */
public interface RouterContext extends AutoCloseable,
    Map<DataStructureKey, Router> {

  @Override
  void close();

  boolean isClosed();

  Router createRouter();

//  public Router get(DataStructureKey sourceKey);
//
//  public boolean containsKey(DataStructureKey sourceKey);
//
//  public Router remove(DataStructureKey sourceKey);
//
//  public Router put(DataStructureKey sourceKey, Router router);
//
//  public Set<DataStructureKey> keySet();

}
