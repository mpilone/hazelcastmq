
package org.mpilone.hazelcastmq.core;

import java.nio.channels.Selector;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>
 * A {@link Condition} that contains a {@link signalled} flag to remember if it
 * has been signalled since the last call to await. If the condition has been
 * signalled, the next call to await will return immediately and clear the
 * signalled state. This is similar to the behavior of {@link Selector} where a
 * call to {@link Selector#wakeup() } is remembered until the next select
 * operation.
 * </p>
 * <p>
 * The goal of this lock is to allow the thread that needs to wait to safely
 * release the lock, do some processing, regain the lock, and await without
 * having to worry about missing any signal calls while the lock was not held.
 * </p>
 * <p>
 * Unlike a {@link Condition} obtained from a {@link ReentrantLock}, this
 * condition can be used outside of a lock and will maintain all thread safety
 * and locking state internally.
 * </p>
 *
 * @author mpilone
 */
class StatefulCondition implements Condition {

  private final ReentrantLock guard = new ReentrantLock();
  private final Condition guardCondition = guard.newCondition();
  private final AtomicBoolean signalled = new AtomicBoolean(false);

  @Override
  public void await() throws InterruptedException {
    guard.lock();
    try {
      if (!signalled.compareAndSet(true, false)) {
        guardCondition.await();
        signalled.set(false);
      }
    }
    finally {
      guard.unlock();
    }
  }

  @Override
  public void awaitUninterruptibly() {
    guard.lock();
    try {
      if (!signalled.compareAndSet(true, false)) {
        guardCondition.awaitUninterruptibly();
        signalled.set(false);
      }
    }
    finally {
      guard.unlock();
    }
  }

  @Override
  public long awaitNanos(long nanosTimeout) throws InterruptedException {
    guard.lock();
    long remaining = 0;
    try {
      if (!signalled.compareAndSet(true, false)) {
        remaining = guardCondition.awaitNanos(nanosTimeout);
        signalled.set(false);
      }
    }
    finally {
      guard.unlock();
    }

    return remaining;
  }

  @Override
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    guard.lock();
    boolean complete = true;
    try {
      if (!signalled.compareAndSet(true, false)) {
        complete = guardCondition.await(time, unit);
        signalled.set(false);
      }
    }
    finally {
      guard.unlock();
    }

    return complete;
  }

  @Override
  public boolean awaitUntil(Date deadline) throws InterruptedException {
    guard.lock();
    boolean complete = true;
    try {
      if (!signalled.compareAndSet(true, false)) {
        complete = guardCondition.awaitUntil(deadline);
        signalled.set(false);
      }
    }
    finally {
      guard.unlock();
    }

    return complete;
  }

  @Override
  public void signal() {
    guard.lock();
    try {
      signalled.set(true);
      guardCondition.signal();
    }
    finally {
      guard.unlock();
    }
  }

  @Override
  public void signalAll() {
    guard.lock();
    try {
      signalled.set(true);
      guardCondition.signalAll();
    }
    finally {
      guard.unlock();
    }
  }

}
