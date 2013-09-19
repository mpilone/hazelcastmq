package org.mpilone.hazelcastmq.core;

public interface HazelcastMQProducer {

  public void send(String destination, byte[] body);

  public void send(String destination, String body);

  public void send(String destination, HazelcastMQMessage msg);

  public HazelcastMQProducer setReplyTo(String destination);

  public HazelcastMQProducer setCorrelationID(String id);

  public HazelcastMQProducer setTimeToLive(long millis);
}
