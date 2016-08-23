
package org.mpilone.hazelcastmq.core;


import org.mpilone.hazelcastmq.core.DefaultRouterContext.RouterData;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * @author mpilone
 */
class BlockingRouterExecutor extends MessageSentMapAdapter implements
    RouterExecutor {

  private final BrokerConfig config;
  private final HazelcastInstance hazelcastInstance;

  public BlockingRouterExecutor(BrokerConfig config) {
    this.config = config;
    this.hazelcastInstance = config.getHazelcastInstance();
  }

  @Override
  void messageSent(DataStructureKey channelKey) {

    IMap<DataStructureKey, RouterData> map = hazelcastInstance.getMap(
        DefaultRouterContext.ROUTER_DATA_MAP_NAME);

    if (map.containsKey(channelKey)) {
      map.lock(channelKey);
      try (DefaultRouter router = new DefaultRouter(channelKey, child -> {
      }, config)) {
        router.routeMessages();
      }
      finally {
        map.unlock(channelKey);
      }
    }
  }




}
