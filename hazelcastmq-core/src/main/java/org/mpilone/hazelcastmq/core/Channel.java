package org.mpilone.hazelcastmq.core;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
public interface Channel extends Closeable {

  void nack(String msgId);

  void nackAll();

  void ack(String msgId);

  void ackAll();

  Message<?> receive();

  Message<?> receive(long timeout, TimeUnit unit);

  boolean send(Message<?> msg);

  boolean send(Message<?> msg, long timeout, TimeUnit unit);

  void addReadReadyListener(ReadReadyListener listener);

  void removeReadReadyListener(ReadReadyListener listener);

  boolean isClosed();

  void markTemporary();

  boolean isTemporary();

  void setAckMode(AckMode ackMode);

  AckMode getAckMode();

  DataStructureKey getChannelKey();

  @Override
   void close();

}
