package org.mpilone.hazelcastmq.core;

import java.io.Closeable;

/**
 *
 * @author mpilone
 */
public interface ChannelContext extends Closeable {

  Channel createChannel(DataStructureKey key);

  void setAutoCommit(boolean autoCommit);

  boolean getAutoCommit();

  void commit();

  void rollback();

}
