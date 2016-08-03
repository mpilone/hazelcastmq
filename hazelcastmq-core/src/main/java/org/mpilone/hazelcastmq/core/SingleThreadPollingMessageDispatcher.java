
package org.mpilone.hazelcastmq.core;

import java.util.Objects;
import java.util.concurrent.*;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
public class SingleThreadPollingMessageDispatcher implements MessageDispatcher {

  /**
   * The log for this class.
   */
  private static final ILogger log = Logger.getLogger(
      SingleThreadPollingMessageDispatcher.class);

  private DataStructureKey channelKey;
  private ExecutorService executor;
  private StoppableTask<Void> dispatchTask;
  private Broker broker;
  private Performer performer;
  private volatile boolean stopped = true;

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

    dispatchTask = new StoppableTask<>(Executors.callable(new DispatchTask(),
        null));
    executor.submit(dispatchTask);
  }

  @Override
  public void stop() {
    if (stopped) {
      return;
    }

    stopped = true;
    try {
      dispatchTask.stop();
      dispatchTask = null;
    }
    catch (InterruptedException ex) {
      log.finest("Interrupted while waiting on dispatch thread to stop.", ex);
      Thread.currentThread().interrupt();
    }
  }

  public void setPerformer(Performer performer) {
    requireStopped();

    this.performer = performer;
  }

  public void setBroker(Broker broker) {
    requireStopped();

    this.broker = broker;
  }

  public void setChannelKey(DataStructureKey channelKey) {
    requireStopped();

    this.channelKey = channelKey;
  }

  public void setExecutor(ExecutorService executor) {
    requireStopped();

    this.executor = executor;
  }

  private void requireStopped() {
      if (!stopped) {
        throw new HazelcastMQException(
            "Dispatcher must be stopped for this operation.");
      }
  }

  private class DispatchTask implements Runnable {

    @Override
    public void run() {

      try (ChannelContext context = broker.createChannelContext();
          Channel channel = context.createChannel(channelKey)) {

        while (!stopped) {
          doDispatch(channel);
        }
      }
    }

    private void doDispatch(Channel channel) {
      log.finest("doDispatch start");

      try {
        Message<?> msg = channel.receive(10, TimeUnit.SECONDS);
        if (msg != null) {
          performer.perform(msg);
        }
      }
      catch (Throwable ex) {
        log.warning("Exception during dispatch. The message will "
            + "be discarded.", ex);
      }

      log.finest("doDispatch end");
    }
  }
}
