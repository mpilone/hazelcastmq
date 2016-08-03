
package org.mpilone.hazelcastmq.core;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
class StoppableTask<T> implements Callable<T> {

  private final CountDownLatch latch;
  private final Object lifecycleMutex = new Object();
  private final Callable<T> callable;
  private final T stoppedResult;

  private Thread thread;
  private boolean stopped;

  public StoppableTask(Callable<T> callable) {
    this(callable, null);
  }

  public StoppableTask(Callable<T> callable, T stoppedResult) {
    this.latch = new CountDownLatch(1);
    this.callable = callable;
    this.stoppedResult = stoppedResult;
  }

  @Override
  public T call() throws Exception {
    boolean s;
    synchronized (lifecycleMutex) {
      s = this.stopped;

      if (!s) {
        this.thread = Thread.currentThread();
      }
    }

    try {
      return s ? stoppedResult : callable.call();
    }
    finally {
      latch.countDown();
    }
  }

  private void doStop() {
    synchronized (lifecycleMutex) {
      stopped = true;
      if (thread != null) {
        thread.interrupt();
      }
      thread = null;
    }
  }

  public void stop() throws InterruptedException {
    doStop();
    latch.await();
  }

  public boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
    doStop();
    return latch.await(timeout, unit);
  }

}
