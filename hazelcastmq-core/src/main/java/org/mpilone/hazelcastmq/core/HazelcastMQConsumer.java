package org.mpilone.hazelcastmq.core;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface HazelcastMQConsumer extends Closeable {

  public void close();

  public HazelcastMQMessageListener getMessageListener();

  public HazelcastMQMessage receive();

  public HazelcastMQMessage receive(long timeout, TimeUnit unit);

  public HazelcastMQMessage receiveNoWait();

  public byte[] receiveBody(long timeout, TimeUnit unit);

  public byte[] receiveBodyNoWait();

  public void setMessageListener(HazelcastMQMessageListener listener);

}
