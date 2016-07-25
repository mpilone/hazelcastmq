package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseQueue;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
class QueueChannel implements Channel {

  private final DefaultChannelContext context;
  private final DataStructureKey key;
  private final HazelcastInstance hazelcastInstance;

  public QueueChannel(DefaultChannelContext context, DataStructureKey key) {

    this.context = context;
    this.key = key;
    this.hazelcastInstance = context.getBroker().getConfig()
        .getHazelcastInstance();
  }

  @Override
  public void nack(String msgId) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void nackAll() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void ack(String msgId) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void ackAll() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Message<?> receive() {
    return receive(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  @Override
  public Message<?> receive(long timeout, TimeUnit unit) {
    Message<?> msg = null;
    try {
      BaseQueue<Message<?>> queue = getTxOrNormalQueue(config
          .getDataStructureKey().getName());
      msg = queue.poll(timeout, unit);

      // TODO: queue in-flight if not auto-ack
    }
    catch (InterruptedException ex) {
      // Ignore and return no message available.
    }

    return msg;
  }

  @Override
  public boolean send(Message<?> msg) {
    return send(msg, Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  @Override
  public boolean send(
      Message<?> msg, long timeout, TimeUnit unit) {

    boolean result = false;

    try {
      BaseQueue<Message<?>> queue = getTxOrNormalQueue(config
          .getDataStructureKey().getName());
      result = queue.offer(msg, timeout, unit);
    }
    catch (InterruptedException ex) {
      // Ignore and return false for send result.
    }

    return result;
  }

  @Override
  public SelectionKey registerSelector(Selector selector) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }


  private BaseQueue<Message<?>> getTxOrNormalQueue(String name) {

    if (config.getTransactionalTaskContext() != null) {
      return config.getTransactionalTaskContext().getQueue(name);
    } else {
      return hazelcastInstance.getQueue(name);
    }
  }

  @Override
  public void setTemporary(boolean temporary) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isTemporary() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setAckMode(AckMode ackMode) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public AckMode getAckMode() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
