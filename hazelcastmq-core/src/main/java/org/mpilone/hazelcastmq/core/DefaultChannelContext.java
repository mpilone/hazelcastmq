package org.mpilone.hazelcastmq.core;

import com.hazelcast.collection.impl.queue.QueueService;
import java.io.IOException;

import static java.lang.String.format;

/**
 *
 * @author mpilone
 */
public class DefaultChannelContext implements ChannelContext {

  private final DefaultBroker broker;

  public DefaultChannelContext(DefaultBroker broker) {
    this.broker = broker;
  }

  @Override
  public Channel createChannel(DataStructureKey key) {
    switch (key.getServiceName()) {
      case QueueService.SERVICE_NAME:
        return new QueueChannel(this, key);

      default:
        throw new UnsupportedOperationException(format(
            "Service type [%s] is not currently supported.", key
            .getServiceName()));
    }
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean getAutoCommit() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void commit() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void rollback() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public DefaultBroker getBroker() {
    return broker;
  }
}
