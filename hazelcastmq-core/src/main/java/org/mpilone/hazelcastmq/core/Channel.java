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

  SelectionKey registerSelector(Selector selector);

  void setTemporary(boolean temporary);

  boolean isTemporary();

  void setAckMode(AckMode ackMode);

  AckMode getAckMode();
}
