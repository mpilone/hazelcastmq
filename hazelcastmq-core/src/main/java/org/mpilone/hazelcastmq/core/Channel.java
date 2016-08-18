package org.mpilone.hazelcastmq.core;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A channel carries a message from a sender to a receiver and is backed by a
 * Hazelcast data structure. A channel is NOT thread-safe and may only be used
 * by a single thread with the exception of the {@link #close() } method that
 * can be called from another thread to interrupt any blocking send or receive
 * operation on the channel. A single channel instance may be used for both
 * sending and receiving (but not concurrently). Asynchronous message processing
 * can be done by adding a {@link ReadReadyListener} to a channel and performing
 * a {@link #receive() } operation after being notified. This pattern is
 * implemented by various {@link MessageDispatcher}s.
 * </p>
 * <p>
 * A channel participates in the transaction managed by the context that created
 * it (when supported).
 * </p>
 * <p>
 * Refer to the EIP Message Channel for more information:
 * http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageChannel.html
 * </p>
 *
 * @author mpilone
 */
public interface Channel extends Closeable {

  void nack(String msgId);

  void nackAll();

  void ack(String msgId);

  void ackAll();

  /**
   * Receives a message from this channel, blocking indefinitely until a message
   * is available, the thread is interrupted, or the channel is closed.
   *
   * @return the message received or null if no message was available and the
   * thread was interrupted or the channel was closed
   */
  Message<?> receive();

  /**
   * Receives a message from this channel, blocking for the given timeout until
   * a message is available, the thread is interrupted, or the channel is
   * closed. A timeout of 0 indicates no wait.
   *
   * @param timeout the amount of time to wait for a message
   * @param unit the unit of the timeout value
   *
   * @return the message received or null if no message was available and the
   * timeout expired, the thread was interrupted, or the channel closed
   */
  Message<?> receive(long timeout, TimeUnit unit);

  /**
   * Sends the given message, blocking indefinitely if the underlying data
   * structure is full and cannot handle more items.
   *
   * @param msg the message to send
   *
   * @return true if the message was sent to the channel successfully, false if
   * the message was rejected by the underlying data structure or a blocking
   * wait was interrupted
   */
  boolean send(Message<?> msg);

  /**
   * Sends the given message, blocking up to the given timeout value if the
   * underlying data structure is full and cannot handle more items. A timeout
   * of 0 indicates no wait.
   *
   * @param msg the message to send
   * @param timeout the amount of time to wait for the data structure to accept
   * the message
   * @param unit the unit of the timeout value
   *
   * @return true if the message was sent to the channel successfully, false if
   * the message was rejected by the underlying data structure, the timeout
   * occurred, or a blocking wait was interrupted
   */
  boolean send(Message<?> msg, long timeout, TimeUnit unit);

  /**
   * <p>
   * Adds the read-ready listener to this channel. The listener will be notified
   * when there may be a message that can be read from the channel. Note that if
   * there multiple consumers on a channel (in a single JVM or multiple JVMs),
   * the message may already be received and no longer available on the channel.
   * Therefore a read-ready notification does not guarantee message availability
   * so a non-blocking receive call is recommended.
   * </p>
   * <p>
   * The listener will be notified in a separate, Hazelcast controlled thread.
   * It is recommended that the receive call not be done in this thread because
   * it will block the notification to other listeners and could cause thread
   * safety issues with this channel.
   * </p>
   * <p>
   * Listeners are not persistent and will be removed when the channel is
   * closed.
   * </p>
   *
   * @param listener the listener to add
   */
  void addReadReadyListener(ReadReadyListener listener);

  /**
   * Removes the read-ready listener from this channel.
   *
   * @param listener the listener to remove
   */
  void removeReadReadyListener(ReadReadyListener listener);

  /**
   * Returns true if the channel is closed.
   *
   * @return true if the channel is closed
   */
  boolean isClosed();

  /**
   * Marks the channel as temporary. When a temporary channel is closed, or more
   * specifically when the context owning the channel is closed, all data
   * structures backing temporary channels will be destroyed. Once a channel is
   * marked as temporary, it cannot be un-marked.
   *
   */
  void markTemporary();

  /**
   * Returns true if this channel has been marked as temporary.
   *
   * @return true if temporary, false otherwise
   */
  boolean isTemporary();

  /**
   * <p>
   * Sets the acknowledgement mode of the channel. By default all channels will
   * be in {@link AckMode#AUTO} which means a message returned by a {@link #receive()
   * } operation is considered automatically acknowledged and requires no
   * further action. In any other acknowledgement mode, messages are considered
   * "in-flight" until they are explicitly {@link #ack(java.lang.String) acked}
   * or {@link #nack(java.lang.String) nacked}. Messages that are in-flight past
   * a configured expiration period will be automatically redelivered to the
   * channel.
   * </p>
   * <p>
   * It is recommended to use auto acknowledgement mode when possible as it
   * offers the best performance. Acks and nacks will be part of any active
   * transaction so if the transaction is rolled back, the acks and nacks are
   * also rolled back with the assumption that any message receive operations
   * are also rolled back and the messages are returned to the channel. Due to
   * the complexity of transactions and acks/nacks, it is recommended that they
   * not be used together but it is supported. That is, use client acks/nacks
   * with auto-commit transactions or auto ack with manual transactions.
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
   * Returns the data structure key of the data structure backing this channel.
   *
   * @return the data structure key
   */
  DataStructureKey getChannelKey();

  /**
   * <p>
   * Closes the channel. This method blocks until all the resources used by the
   * channel are closed and released. Any blocking send or receive call will be
   * interrupted and allowed to return before this method returns. This is the
   * only method on a channel that can be safely called by multiple threads.
   * </p>
   * <p>
   * Closing a channel does not destroy the backing distributed object nor any
   * of the data in the data structure but simply closes this reference to the
   * data structure. Multiple channel instances can exist concurrently for the
   * same channel key and continue to work with the data in the channel as
   * instances are created and closed. To completely destroy a channel and the
   * backing data, use the {@link ChannelContext#destroyChannel(org.mpilone.hazelcastmq.core.DataStructureKey)
   * } method or mark the channel as temporary using {@link #markTemporary() }
   * and it will be automatically destroyed when the owning channel context is
   * closed.
   * </p>
   */
  @Override
   void close();

}
