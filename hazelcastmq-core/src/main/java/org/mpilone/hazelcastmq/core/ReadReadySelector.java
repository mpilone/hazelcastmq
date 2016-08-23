
package org.mpilone.hazelcastmq.core;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * <p>
 * A {@link ReadReadyListener} that acts like a selector on the channels the
 * listener is added to. This allows a single thread to monitor multiple
 * channels for a read-ready status change and then process the messages on
 * those channels.
 * </p>
 * <p>
 * This class is thread-safe however it is up to the caller to ensure that the
 * thread-safety requirements of the channels returned from a select operation
 * is properly honored.
 * </p>
 *
 * @author mpilone
 */
public class ReadReadySelector implements ReadReadyListener {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(ReadReadySelector.class);

  private final Lock channelLock;
  private final Condition readReadyCondition;
  private final Collection<Channel> channels;

  /**
   * Constructs the selector.
   */
  public ReadReadySelector() {
    this.channelLock = new ReentrantLock(true);
    this.readReadyCondition = channelLock.newCondition();
    this.channels = new ArrayList<>();
  }

  /**
   * <p>
   * Returns the list of channels that have indicated that they are read-ready
   * or blocks indefinitely until a channel is read-ready, the thread is
   * interrupted, or {@link #wakeup() } is called.
   * </p>
   *
   * @return the read-ready channels or an empty list if interrupted
   * @see #select(long, java.util.concurrent.TimeUnit)
   */
  public Collection<Channel> select() {
    return select(Integer.MAX_VALUE, TimeUnit.MINUTES);
  }

  /**
   * <p>
   * Returns the list of channels that have indicated that they are read-ready
   * or blocks for the given timeout until a channel is read-ready, the thread
   * is interrupted, or {@link #wakeup() } is called.
   * </p>
   * <p>
   * Even though a channel may be returned by this operation because it
   * indicated that it is read-ready, it is possible that some other consumer on
   * the channel has already received any pending messages and therefore there
   * are no more messages available. Therefore it is recommended to do a
   * non-blocking receive on the returned channels.
   * </p>
   * <p>
   * If there are multiple threads blocking on a select, only one will return
   * when a channel indicates that it is read-ready. The order of the blocking
   * selectors is fair.
   * </p>
   *
   * @param timeout the amount of time to block
   * @param unit the unit of the timeout value
   *
   * @return the read-ready channels or an empty list if interrupted
   */
  public Collection<Channel> select(long timeout, TimeUnit unit) {
    Collection<Channel> result;
    channelLock.lock();
    try {
      if (channels.isEmpty()) {
        readReadyCondition.await();
      }

      result = new ArrayList<>(channels);
      channels.clear();
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      result = Collections.emptyList();
    }
    finally {
      channelLock.unlock();
    }

    return result;
  }

  @Override
  public void readReady(ReadReadyEvent event) {
    channelLock.lock();
    try {
      channels.add(event.getChannel());
      readReadyCondition.signal();
    }
    finally {
      channelLock.unlock();
    }
  }

  /**
   * Wakes up any threads blocked in a {@link #select() } call. This is similar
   * to simply interrupting the threads but does not require tracking the
   * specific selecting thread(s).
   */
  public void wakeup() {
    channelLock.lock();
    try {
      readReadyCondition.signalAll();
    }
    finally {
      channelLock.unlock();
    }
  }

}
