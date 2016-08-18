package org.mpilone.hazelcastmq.core;

import java.util.Set;

/**
 *
 * @author mpilone
 */
public interface RouterContext extends AutoCloseable {

  @Override
  void close();

  boolean isClosed();

  Router createRouter(DataStructureKey channelKey);

  boolean containsRouterChannelKey(DataStructureKey channelKey);

  Set<DataStructureKey> routerChannelKeySet();

  boolean destroyRouter(DataStructureKey channelKey);

}
