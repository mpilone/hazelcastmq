package org.mpilone.hazelcastmq.core;

import java.io.Closeable;

public interface HazelcastMQContext extends Closeable {
  public HazelcastMQConsumer createConsumer(String destination);

  public HazelcastMQProducer createProducer();

  public void close();

  public void commit();

  public void rollback();

  public boolean isTransacted();

  public void start();

  public void stop();

  public String createTemporaryQueue();

  public String createTemporaryTopic();

  public void destroyTemporaryDestination(String destination);

  public boolean isAutoStart();

  public void setAutoStart(boolean autoStart);
}
