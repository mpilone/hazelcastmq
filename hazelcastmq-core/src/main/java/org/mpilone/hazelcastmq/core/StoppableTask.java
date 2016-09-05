
package org.mpilone.hazelcastmq.core;

import java.util.concurrent.*;

/**
 * <p>
 * A {@link Callable} task that can be stopped safely. If the task has not yet
 * started when stopped, the target callable will not run. If the task is
 * already started when stopped, the target callable will be interrupted and the
 * stop method can block.
 * </p>
 * <p>
 * This class is a simplified version of {@link FutureTask} but he stop method
 * blocks unlike the cancel method of FutureTask. This greatly simplifies the
 * stopping behavior assuming the target callable properly honors thread
 * interrupts.
 * </p>
 *
 * @author mpilone
 */
class StoppableTask<T> implements Callable<T>, Stoppable {

  private final CountDownLatch latch;
  private final Object lifecycleMutex = new Object();
  private final Callable<T> callable;
  private final T stoppedResult;

  private Thread thread;
  private boolean stopped;

  /**
   * Constructs the task with the given target callable. A null value will be
   * returned by this task if it is stopped before it is started.
   *
   * @param callable the callable to execute when this task is run
   */
  public StoppableTask(Callable<T> callable) {
    this(callable, null);
  }

  /**
   * Constructs the task with the given target callable and result if stopped
   * before it is started.
   *
   * @param callable the callable to execute when this task is run
   * @param stoppedResult the result value to return if the task is stopped
   * before being run
   */
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

  /**
   * Performs the stop by interrupting the thread executing the task.
   */
  private void doStop() {
    synchronized (lifecycleMutex) {
      stopped = true;
      if (thread != null) {
        thread.interrupt();
      }
      thread = null;
    }
  }

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @throws InterruptedException if interrupted while awaiting termination
   */
  public void stop() throws InterruptedException {
    doStop();
    latch.await();
  }

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @param timeout the amount of time to wait for termination
   * @param unit the unit of the time value
   *
   * @return true if the task terminated in the given time, false otherwise
   * @throws InterruptedException if interrupted while awaiting termination
   */
  public boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
    doStop();
    return latch.await(timeout, unit);
  }

}
