package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;

/**
 *
 * @author mpilone
 */
public class BrokerConfig {

  private HazelcastInstance hazelcastInstance;

  public BrokerConfig() {
  }

  public BrokerConfig(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public HazelcastInstance getHazelcastInstance() {
    return hazelcastInstance;
  }

  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

}
