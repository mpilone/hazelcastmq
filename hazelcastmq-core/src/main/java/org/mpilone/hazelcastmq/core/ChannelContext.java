package org.mpilone.hazelcastmq.core;

/**
 * <p>
 * A context to create {@link Channel} instances and control the transaction
 * around sending and receiving messages. A channel context is NOT thread-safe
 * and is intended to be used by a single thread. A context should be closed
 * when it is no longer needed to release resources.
 * </p>
 * <p>
 * A context has a single transaction that all channels participate in. The
 * context always defaults to auto-commit which means the {@link #commit() } and {@link #rollback()
 * } methods should not be used directly and all channel operations are
 * committed immediately (in most cases, no transaction is used at all).
 * Auto-commit can be disabled using the {@link #setAutoCommit(boolean) }
 * method.
 * </p>
 *
 * @author mpilone
 */
public interface ChannelContext extends AutoCloseable {

  /**
   * Creates a new channel that will send and receive messages using the data
   * structure identified with the channel key.
   *
   * @param channelKey the channel key used to identify the data structure
   * backing the channel
   *
   * @return a new channel instance
   */
  Channel createChannel(DataStructureKey channelKey);

  /**
   * Destroys the given channel by destroying the backing distributed object and
   * all associated data in the data structure. If a channel with the given key
   * is used or created again, the data structure will be recreated
   * automatically. There is no difference between this method and simply
   * getting the distributed object directly out of Hazelcast and destroying it.
   * This method exists primarily to provide symmetry with the routing context.
   *
   * @param channelKey the channel key used to identify the data structure to
   * destroy
   *
   * @return true if the data structure existed and was destroyed
   */
  boolean destroyChannel(DataStructureKey channelKey);

  /**
   * Enables or disables auto-commit for this channel context. If there are
   * pending transactional operations, the result of those operations are
   * undefined (they may be committed or rolled back). Therefore it is
   * recommended to configure auto-commit before creating channels or performing
   * operations on those channels.
   *
   * @param autoCommit true to enable auto-commit (the default), false to
   * disable it
   */
  void setAutoCommit(boolean autoCommit);

  /**
   * Returns the current auto-commit setting.
   *
   * @return true if auto-commit is enabled, false otherwise
   */
  boolean getAutoCommit();

  /**
   * Commits the current transaction if auto-commit is disabled. A new
   * transaction is automatically started.
   */
  void commit();

  /**
   * Rolls back the current transaction if auto-commit is disabled. A new
   * transaction is automatically started.
   */
  void rollback();

  /**
   * Closes the channel context and all channels created by the context. This
   * method blocks until all channels and resources are closed. If there are
   * pending transactional operations, the result of those operations are
   * undefined (they may be committed or rolled back).
   */
  @Override
  void close();

}
