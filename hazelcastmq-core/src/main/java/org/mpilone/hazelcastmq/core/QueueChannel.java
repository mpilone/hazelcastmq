package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.mpilone.hazelcastmq.core.Message;

import static java.lang.String.format;

/**
 * Channel implementation backed by a {@link IQueue}. This implementation is
 * fully transactional.
 *
 * @author mpilone
 */
class QueueChannel implements Channel {

  private final static ILogger log = Logger.getLogger(QueueChannel.class);

  private final TrackingParent<Channel> parent;
  private final DataStructureContext dataStructureContext;
  private final InflightContext inflightContext;
  private final DataStructureKey channelKey;
  private final Object taskMutex;
  private final Object readReadyMutex;
  private final Collection<ReadReadyListener> readReadyListeners;
  private final MessageConverter messageConverter;

  private String messageSentRegistrationId;
  private StoppableTask<Boolean> sendTask;
  private StoppableTask<Message<?>> receiveTask;
  private volatile boolean closed;
  private boolean temporary;
  private Clock clock = Clock.systemUTC();

  public QueueChannel(DataStructureKey channelKey,
      TrackingParent<Channel> parent,
      DataStructureContext dataStructureContext,
      InflightContext inflightContext,
      BrokerConfig config) {

    this.parent = parent;
    this.channelKey = channelKey;
    this.messageConverter = config.getMessageConverter();
    this.taskMutex = new Object();
    this.readReadyMutex = new Object();
    this.readReadyListeners = new HashSet<>(2);
    this.dataStructureContext = dataStructureContext;
    this.inflightContext = inflightContext;
  }

  @Override
  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  /**
   * Checks if the channel is closed and throws an exception if it is.
   *
   * @throws HazelcastMQException if the context is closed
   */
  private void requireNotClosed() throws HazelcastMQException {
    if (closed) {
      throw new HazelcastMQException("Channel is closed.");
    }
  }

  @Override
  public Message<?> receive() {
    return receive(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  @Override
  public Message<?> receive(long timeout, TimeUnit unit) {
    requireNotClosed();

    synchronized (taskMutex) {
      if (receiveTask != null) {
        throw new HazelcastMQException("Receive is already in progress.");
      }

      receiveTask = new StoppableTask(new ReceiveLogicBusyLoop(timeout, unit),
          null);
    }

    Message<?> result;
    try {
      result = receiveTask.call();
    }
    catch (InterruptedException ex) {
      // Poll was interrupted while waiting. Treat it like a cancel.
      Thread.currentThread().interrupt();
      result = null;
    }
    catch (Exception ex) {
      throw new HazelcastMQException("Error while trying to receive.", ex);
    }

    synchronized (taskMutex) {
      receiveTask = null;
    }

    return result;
  }

  @Override
  public boolean send(Message<?> msg) {
    return send(msg, Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  @Override
  public boolean send(
      Message<?> msg, long timeout, TimeUnit unit) {
    requireNotClosed();

    synchronized (taskMutex) {
      if (sendTask != null) {
        throw new HazelcastMQException("Send is already in progress.");
      }

      sendTask = new StoppableTask<>(new SendLogicBusyLoop(msg, timeout, unit),
          false);
    }

    boolean result;
    try {
      result = sendTask.call();
    }
    catch (InterruptedException ex) {
      // Put was interrupted while waiting. Treat it like a cancel.
      Thread.currentThread().interrupt();
      result = false;
    }
    catch (Exception ex) {
      throw new HazelcastMQException("Error while trying to send.", ex);
    }

    synchronized (taskMutex) {
      sendTask = null;
    }

    return result;
  }

  @Override
  public void addReadReadyListener(ReadReadyListener listener) {
    // Delay adding the item listener until we get our first read-ready
    // listener. This allows for light weight channels that just use
    // polling and therefore may never need to use an item listener at all.
    if (messageSentRegistrationId == null) {

      final ReadReadyNotifier notifier = new ReadReadyNotifier();

      messageSentRegistrationId = MessageSentAdapter.getMapToListen(
          dataStructureContext).addEntryListener(notifier,
              channelKey, false);
    }

    // Synchronize because the listeners will be notified from a HZ
    // thread.
    synchronized (readReadyMutex) {
      readReadyListeners.add(listener);
    }
  }

  @Override
  public void removeReadReadyListener(ReadReadyListener listener) {
    synchronized (readReadyListeners) {
      readReadyListeners.remove(listener);
    }

    if (readReadyListeners.isEmpty() && messageSentRegistrationId
        != null) {
      // Stop listening for events. This is a slight optimization to
      // avoid getting events that we'll never notify listeners of.
      MessageSentAdapter.getMapToListen(dataStructureContext).
          removeEntryListener(messageSentRegistrationId);

      messageSentRegistrationId = null;
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    synchronized (readReadyMutex) {
      readReadyListeners.clear();
    }

    if (messageSentRegistrationId != null) {
      // Stop listening for item events.
      MessageSentAdapter.getMapToListen(dataStructureContext).
          removeEntryListener(messageSentRegistrationId);

      messageSentRegistrationId = null;
    }

    synchronized (taskMutex) {
      if (sendTask != null) {
        try {
          sendTask.stop();
        }
        catch (InterruptedException ex) {
          // Ignore because we're closing..
        }
      }

      if (receiveTask != null) {
        try {
          receiveTask.stop();
        }
        catch (InterruptedException ex) {
          // Ignore because we're closing.
        }
      }
    }

    parent.remove(this);
  }

  @Override
  public void markTemporary() {
    temporary = true;
  }

  @Override
  public boolean isTemporary() {
    return temporary;
  }

  private class ReadReadyNotifier extends MessageSentAdapter {

    @Override
    public void messageSent(DataStructureKey channelKey) {

      synchronized (readReadyMutex) {
        // Clone the list to avoid concurrent modification exception.
        new ArrayList<>(readReadyListeners).stream().forEach(
            l -> l.readReady(new ReadReadyEvent(QueueChannel.this)));
      }
    }

  }

  /**
   * The logic to send a message to the backing queue. Ideally this logic would
   * simply call {@link BaseQueue#offer(java.lang.Object) } and block until the
   * message could be sent or until interrupted but Hazelcast's blocking
   * operations do not return when interrupted because they must wait until the
   * distributed operation is complete. Until this behavior changes a busy loop
   * is used to offer in short intervals and the thread interrupt status is
   * checked on each iteration of the loop.
   */
  private class SendLogicBusyLoop implements Callable<Boolean> {

    private final Message<?> msg;
    private final CountdownTimer timer;

    public SendLogicBusyLoop(Message<?> msg, long timeout, TimeUnit unit) {
      this.msg = msg;
      this.timer = new CountdownTimer(timeout, unit);
    }

    @Override
    public Boolean call() throws Exception {
      final BaseQueue<Object> queue = dataStructureContext.
          getQueue(channelKey.getName(), true);

      // Convert to a raw message object.
      final Object rawMsg = messageConverter.fromMessage(msg);

      boolean success = false;
      boolean interrupted;

      timer.reset();
      timer.start();

      do {

        // Offer to the queue.
        success = queue.offer(rawMsg, timer.getRemainingOrInterval(1,
            TimeUnit.SECONDS), TimeUnit.MILLISECONDS);

        interrupted = Thread.interrupted();

      } while (!interrupted && !success && !timer.isExpired());

      timer.stop();

      if (!success && interrupted) {
        throw new InterruptedException();
      }
      else if (success) {
        MessageSentAdapter.getMapToPut(dataStructureContext, true).put(
            channelKey, clock.millis());
      }

      return success;
    }

  }

  /**
   * The logic to receive a message from the backing queue. Ideally this logic
   * would simply call {@link BaseQueue#poll() } and block until a message was
   * available or until interrupted but Hazelcast's blocking operations do not
   * return when interrupted because they must wait until the distributed
   * operation is complete. Until this behavior changes a busy loop is used to
   * poll in short intervals and the thread interrupt status is checked on each
   * iteration of the loop.
   */
  private class ReceiveLogicBusyLoop implements Callable<Message<?>> {

    private final CountdownTimer timer;

    public ReceiveLogicBusyLoop(long timeout, TimeUnit unit) {
      this.timer = new CountdownTimer(timeout, unit);
    }

    @Override
    public Message<?> call() throws Exception {

      final BaseQueue<Message<?>> queue =
          dataStructureContext.getQueue(channelKey.getName(), true);

      Message<?> msg = null;
      boolean interrupted;

      timer.reset();
      timer.start();

      do {

        final Object rawMsg = queue.poll(timer.getRemainingOrInterval(1,
            TimeUnit.SECONDS), TimeUnit.MILLISECONDS);

        if (rawMsg != null) {
          // Convert to a message.
          msg = messageConverter.toMessage(rawMsg);

          // Check for message expiration.
          Long expiration = (Long) msg.getHeaders().get(
              MessageHeaders.EXPIRATION);
          if (expiration != null && Instant.now(clock).toEpochMilli()
              > expiration) {

            // TODO: it would be nice to move the message to a DLQ.
            log.fine(format("Dropping expired message %s.", msg.getHeaders().
                getId()));

            msg = null;
          }
        }

        interrupted = Thread.interrupted();

      } while (msg == null && !timer.isExpired() && !interrupted);

      timer.stop();

      if (msg == null && interrupted) {
        throw new InterruptedException();
      } else if (msg != null) {

        // Record the inflight message.
        inflightContext.inflight(channelKey, msg);
      }

      return msg;
    }
  }
}
