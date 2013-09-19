package org.mpilone.hazelcastmq.core;

public interface HazelcastMQMessageListener {
  public void onMessage(HazelcastMQMessage msg);
}
