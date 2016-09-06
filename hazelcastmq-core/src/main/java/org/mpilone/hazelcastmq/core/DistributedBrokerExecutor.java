package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.String.format;

/**
 *
 * @author mpilone
 */
class DistributedBrokerExecutor implements Executor, Stoppable {

  /**
   * The log for this class.
   */
  private final static ILogger log = Logger.getLogger(
      DistributedBrokerExecutor.class);

  private final HazelcastInstance hazelcastInstance;
  private final String executorServiceName;
  private final String brokerName;

  private final List<Future<?>> submittedTasks;
  private final Object lifecycleMutex = new Object();

  private boolean stopped;

  public DistributedBrokerExecutor(HazelcastInstance hazelcastInstance,
      String executorServiceName, String brokerName) {
    this.hazelcastInstance = hazelcastInstance;
    this.executorServiceName = executorServiceName;
    this.brokerName = brokerName;
    this.submittedTasks = new LinkedList<>();
  }

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @throws InterruptedException if interrupted while awaiting termination
   */
  @Override
  public void stop() throws InterruptedException {
    stop(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
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
      stopped = true;

      long remaining = unit.toMillis(timeout);

      while (!submittedTasks.isEmpty() && remaining > 0) {
        long start = System.currentTimeMillis();

        try {
          submittedTasks.get(0).get(remaining, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException | ExecutionException ex) {
          // Ignore. We only care about tasks completing and not the
          // final status.
        }
        finally {
          remaining = remaining - (System.currentTimeMillis() - start);
        }

        purgeDoneTasks();
      }
    }

    return submittedTasks.isEmpty();
  }

  @Override
  public void execute(Runnable command) {
    synchronized (lifecycleMutex) {
      if (stopped) {
        return;
      }

      // Only purge when there are a reasonable number of tasks to check
      // so we're not constantly iterating over the submitted task list.
      if (submittedTasks.size() > 10) {
        purgeDoneTasks();
      }

      // Wrap the command to locate the broker.
      command = new BrokerLocatorWrapperTask(command, brokerName);
      Callable<Void> callableCommand = new RunnableWrapperTask(command);

      Future<?> future = submitToExecutorService(callableCommand);
      submittedTasks.add(future);
    }
  }

  private Future<?> submitToExecutorService(Callable<Void> command) {

    // Lookup the executor and the target member. In the future this might
    // be a configuration option.
    final IExecutorService executor = hazelcastInstance.getExecutorService(
        executorServiceName);
    final Member member = hazelcastInstance.getCluster().getLocalMember();

    return executor.submitToMember(command, member);
  }

  private void purgeDoneTasks() {
    for (Iterator<Future<?>> iter = submittedTasks.iterator(); iter.hasNext();) {
      Future<?> task = iter.next();

      if (task.isDone()) {
        iter.remove();
      }
    }
  }

  private static class RunnableWrapperTask implements Callable<Void>,
      Serializable {

    private static final long serialVersionUID = 1L;

    private final Runnable task;

    public RunnableWrapperTask(Runnable task) {
      this.task = task;
    }

    @Override
    public Void call() throws Exception {
      task.run();
      return null;
    }

  }

  private static class BrokerLocatorWrapperTask implements Runnable,
      Serializable {
    private static final long serialVersionUID = 1L;

    private final String brokerName;
    private final Runnable task;

    public BrokerLocatorWrapperTask(Runnable task, String brokerName) {
      this.brokerName = brokerName;
      this.task = task;
    }

    @Override
    public void run() {

      if (task instanceof BrokerAware) {
        final Broker broker = HazelcastMQ.getBrokerByName(brokerName);

        if (broker == null) {
          // Log and return.
          log.warning(format("Unable to find broker %s to auto nack "
              + "inflight messages.", brokerName));

          return;
        }

        ((BrokerAware) task).setBroker(broker);
      }

      // Run the target task.
      task.run();
    }
  }
}
