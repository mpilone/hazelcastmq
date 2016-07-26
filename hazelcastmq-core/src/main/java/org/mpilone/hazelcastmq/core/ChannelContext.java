package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 */
public interface ChannelContext extends AutoCloseable {

  Channel createChannel(DataStructureKey key);

  void setAutoCommit(boolean autoCommit);

  boolean getAutoCommit();

  void commit();

  void rollback();

  @Override
  void close();

}
