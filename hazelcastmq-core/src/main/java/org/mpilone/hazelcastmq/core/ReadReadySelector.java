
package org.mpilone.hazelcastmq.core;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
public class ReadReadySelector implements ReadReadyListener {

  private final static ILogger log = Logger.getLogger(ReadReadySelector.class);

  private final Lock channelLock;
  private final Condition readReadyCondition;
  private final Collection<Channel> channels;

  public ReadReadySelector() {
    this.channelLock = new ReentrantLock(true);
    this.readReadyCondition = channelLock.newCondition();
    this.channels = new ArrayList<>();
  }

  public Collection<Channel> select() {
    return select(Integer.MAX_VALUE, TimeUnit.MINUTES);
  }

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
  public void readReady(Channel channel) {
    channelLock.lock();
    try {
      channels.add(channel);
      readReadyCondition.signal();
    }
    finally {
      channelLock.unlock();
    }
  }

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
