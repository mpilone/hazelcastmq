
package org.mpilone.hazelcastmq.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
public class SingleThreadMultiplexingMessageDispatcher implements MessageDispatcher {

  /**
   * The log for this class.
   */
  private static final ILogger log = Logger.getLogger(SingleThreadMultiplexingMessageDispatcher.class);

  private final Map<DataStructureKey, Performer> performerMap = new HashMap<>();
  private ExecutorService executor;
  private StoppableTask<Void> dispatchTask;
  private Broker broker;
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

  public void setPerformer(DataStructureKey channelKey, Performer performer) {
    requireStopped();

    if (performer == null) {
      performerMap.remove(channelKey);
    }
    else {
      performerMap.put(channelKey, performer);
    }
  }

  public void setBroker(Broker broker) {
    requireStopped();

    this.broker = broker;
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

      try (ChannelContext context = broker.createChannelContext()) {

        final ReadReadySelector selector = new ReadReadySelector();
        final Map<DataStructureKey, Channel> channelMap = new HashMap<>(
            performerMap.size());

        // Create a channel for each key we are multiplexing over and
        // add our selector as a read-ready listener.
        performerMap.keySet().forEach(key -> {
          Channel channel = context.createChannel(key);
          channel.addReadReadyListener(selector);
          channelMap.put(key, channel);
        });

        // Dispatch until we are told to stop.
        while (!stopped) {
          doDispatch(selector);
        }

        // Close all the channels.
        channelMap.values().forEach(Channel::close);
      }
    }

    private void doDispatch(ReadReadySelector selector) {

      log.finest("doDispatch start");

      // Select on the channels.
      selector.select(10, TimeUnit.SECONDS).forEach(channel -> {

        try {
          // Try to receive a message from each of the channels that
          // indicated that they are read-ready. If we get a message, dispatch
          // it to the associated performer.
          Message<?> msg = channel.receive(0, TimeUnit.SECONDS);
          if (msg != null) {
            performerMap.get(channel.getChannelKey()).perform(msg);
          }
        }
        catch (Throwable ex) {
          log.warning("Exception during dispatch. The message will "
              + "be discarded.", ex);
        }
      });

      log.finest("doDispatch stop");
    }
  }
}
