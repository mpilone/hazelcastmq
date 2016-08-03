package org.mpilone.hazelcastmq.core;

import java.util.Objects;
import java.util.concurrent.*;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
public class MultipleThreadPollingMessageDispatcher implements MessageDispatcher {

  /**
   * The log for this class.
   */
  private static final ILogger log = Logger.getLogger(
      MultipleThreadPollingMessageDispatcher.class);

  private DataStructureKey channelKey;
  private ExecutorService executor;
  private CompletionService<Void> completionService;
  private StoppableTask<Void> pollerTask;
  private Broker broker;
  private Performer performer;
  private volatile boolean stopped = true;
  private int maxConcurrentPerformers = 1;

  @Override
  public void start() {
    if (!stopped) {
      return;
    }

    // Check that we are valid to start.
    Objects.requireNonNull(executor,
        "Executor must be set before starting the dispatcher.");
    Objects.requireNonNull(broker,
        "Broker must be set before starting the dispatcher.");
    Objects.requireNonNull(performer,
        "Performer must be set before starting the dispatcher.");
    Objects.requireNonNull(channelKey,
        "Channel key must be set before starting the dispatcher.");

    stopped = false;

    pollerTask = new StoppableTask<>(Executors.
        callable(new DispatchTask(), null));
    executor.submit(pollerTask);
  }

  @Override
  public void stop() {
    if (stopped) {
      return;
    }

    stopped = true;
    try {
      pollerTask.stop();
      pollerTask = null;
    }
    catch (InterruptedException ex) {
      log.finest("Interrupted while waiting on dispatch thread to stop.", ex);
      Thread.currentThread().interrupt();
    }
  }

  public void setMaxConcurrentPerformers(int maxConcurrentPerformers) {
    this.maxConcurrentPerformers = maxConcurrentPerformers;
  }

  public void setExecutor(ExecutorService executor) {
    requireStopped();

    this.executor = executor;
    this.completionService = new ExecutorCompletionService<>(executor);
  }

  public void setBroker(Broker broker) {
    requireStopped();

    this.broker = broker;
  }

  public void setChannelKey(DataStructureKey channelKey) {
    requireStopped();

    this.channelKey = channelKey;
  }

  public void setPerformer(Performer performer) {
    requireStopped();

    this.performer = performer;
  }

  private void requireStopped() {
    if (!stopped) {
      throw new HazelcastMQException(
          "Dispatcher must be stopped for this operation.");
    }
  }

  private class DispatchTask implements Runnable {

    private int activeCount;

    @Override
    public void run() {

      try (ChannelContext context = broker.createChannelContext();
          Channel channel = context.createChannel(channelKey)) {

        while (!stopped) {
          doDispatch(channel);
        }

        while (activeCount > 0 && !Thread.currentThread().isInterrupted()) {
          waitForCompletion();
        }
      }
    }

    private void doDispatch(Channel channel) {

      log.finest("doDispatch start");

      // We're at max active consumers so we'll wait for one of them to finish.
      if (activeCount >= maxConcurrentPerformers) {
        waitForCompletion();
      }

      // The consumer finished or we were interrupted.
      if (!stopped && !Thread.currentThread().isInterrupted()) {

        try {
          Message<?> msg = channel.receive(30, TimeUnit.SECONDS);
          if (msg != null) {
            completionService.submit(new PerformerTask(msg), null);
            activeCount++;
          }
        }
        catch (Throwable ex) {
          log.warning("Exception during dispatch. The message will "
              + "be discarded.", ex);
        }
      }

      log.finest("doDispatch end");
    }

    private void waitForCompletion() {

      try {
        Future<Void> performerTask = completionService.take();
        activeCount--;
        performerTask.get();
      }
      catch (InterruptedException ex) {
        log.finest("Interrupted while waiting performer to complete.", ex);
        Thread.currentThread().interrupt();
      }
      catch (ExecutionException ex) {
        log.warning("Exception during perform execution. The message will "
            + "be discarded.", ex);
      }
    }
  }

  private class PerformerTask implements Runnable {

    private final Message<?> msg;

    public PerformerTask(Message<?> msg) {
      this.msg = msg;
    }

    @Override
    public void run() {
      performer.perform(msg);
    }
  }
}
