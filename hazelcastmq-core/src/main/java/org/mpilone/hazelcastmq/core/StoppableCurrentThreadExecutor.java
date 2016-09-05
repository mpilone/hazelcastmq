package org.mpilone.hazelcastmq.core;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
class StoppableCurrentThreadExecutor implements Executor, Stoppable {

  private final Object lifecycleMutex = new Object();
  private boolean stopped;
  private long runningCount = 0;

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @throws InterruptedException if interrupted while awaiting termination
   */
  @Override
  public void stop() throws InterruptedException {

    synchronized (lifecycleMutex) {
      if (runningCount != 0) {
        lifecycleMutex.wait();
      }

      stopped = true;
    }

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
  @Override
  public boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
    synchronized (lifecycleMutex) {
      if (runningCount != 0) {
        lifecycleMutex.wait(unit.toMillis(timeout));
      }

      stopped = runningCount == 0;
    }

    return stopped;
  }

  @Override
  public void execute(Runnable command) {
    synchronized (lifecycleMutex) {
      if (stopped) {
        return;
      }
      runningCount++;
    }

    try {
    command.run();
    }
    finally {
      synchronized (lifecycleMutex) {
        runningCount--;
        lifecycleMutex.notifyAll();
      }
    }
  }

}
