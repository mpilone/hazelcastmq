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

  /**
   * <p>
   * Sets the acknowledgment mode of the channels created by this context. By
   * default all channels will be in {@link AckMode#AUTO} which means a message
   * returned by a {@link #receive()} operation is considered automatically
   * acknowledged and requires no further action. In any other acknowledgment
   * mode, messages are considered "in-flight" until they are explicitly
   * {@link #ack(java.lang.String) acked} or
   * {@link #nack(java.lang.String) nacked}. Messages that are in-flight past a
   * configured expiration period may be automatically redelivered to the
   * channel.
   * </p>
   * <p>
   * It is recommended to use auto acknowledgment mode when possible as it
   * offers the best performance. Acks and nacks will be part of any active
   * transaction so if the transaction is rolled back, the acks and nacks are
   * also rolled back with the assumption that any message receive operations
   * are also rolled back and the messages are returned to the channel. Due to
   * the complexity of transactions and acks/nacks, it is recommended that they
   * not be used together but it is supported. That is, use client acks/nacks
   * with auto-commit transactions or auto ack with manual transactions.
   * </p>
   * <p>
   * The behavior of in-flight messages when the acknowledgment mode is change
   * is undefined. Therefore it is recommended to set the mode before calling
   * receive on the channel.
   * </p>
   *
   * @param ackMode the desired ack/nack mode
   */
  void setAckMode(AckMode ackMode);

  /**
   * Returns the current ack/nack mode.
   *
   * @return the ack/nack mode
   */
  AckMode getAckMode();

  /**
   * Indicates a negative-acknowledgment (nack) of the messages with the given
   * IDs. In {@link AckMode#AUTO} this method does nothing. In
   * {@link AckMode#CLIENT}, the messages with the given IDs and all previous
   * messages received in the entire context since the last call to {@link #nack(java.lang.String)
   * } or {@link #ack(java.lang.String) } will be nacked. Not passing any
   * message IDs will cause all messages received in the parent context to be
   * nacked.
   *
   * @param msgIds the IDs of the message to nack or null
   */
  void nack(String... msgIds);

  /**
   * Indicates an acknowledgment (ack) of the messages with the given IDs. In
   * {@link AckMode#AUTO} this method does nothing. In {@link AckMode#CLIENT},
   * the messages with the given IDs and all previous messages received in the
   * entire context since the last call to {@link #nack(java.lang.String)
   * } or {@link #ack(java.lang.String) } will be acked. Not passing any message
   * IDs will cause all messages received in the parent context to be acked.
   *
   * @param msgIds the IDs of the message to nack or null
   */
  void ack(String... msgIds);

}
