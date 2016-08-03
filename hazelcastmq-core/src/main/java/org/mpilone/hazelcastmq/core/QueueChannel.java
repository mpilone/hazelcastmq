package org.mpilone.hazelcastmq.core;

import java.util.*;
import java.util.concurrent.*;

import org.mpilone.hazelcastmq.core.Message;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 *
 * @author mpilone
 */
class QueueChannel implements Channel {

  private final static ILogger log = Logger.getLogger(QueueChannel.class);

  private final DefaultChannelContext context;
  private final DataStructureKey channelKey;
  private final HazelcastInstance hazelcastInstance;
  private final Object taskMutex;
  private final Object readReadyMutex;
  private final Collection<ReadReadyListener> readReadyListeners;

  private String readReadyNotifierRegistrationId;
  private StoppableTask<Boolean> sendTask;
  private StoppableTask<Message<?>> receiveTask;
  private volatile boolean closed;
  private boolean temporary;

  public QueueChannel(DefaultChannelContext context, DataStructureKey channelKey) {

    this.context = context;
    this.channelKey = channelKey;
    this.hazelcastInstance = context.getBroker().getConfig()
        .getHazelcastInstance();
    this.taskMutex = new Object();
    this.readReadyMutex = new Object();
    this.readReadyListeners = new HashSet<>(2);
  }

  @Override
  public DataStructureKey getChannelKey() {
    return channelKey;
  }

  @Override
  public void nack(String msgId) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void nackAll() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void ack(String msgId) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void ackAll() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  /**
   * Checks if the context is closed and throws an exception if it is.
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

      sendTask = new StoppableTask<>(new SendLogic(msg, timeout, unit), false);
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
    if (readReadyNotifierRegistrationId == null) {
      readReadyNotifierRegistrationId = hazelcastInstance.getQueue(
          channelKey.getName()).
          addItemListener(new ReadReadyNotifier(), false);
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

    if (readReadyNotifierRegistrationId != null) {
      // Stop listening for item events.
      hazelcastInstance.getQueue(channelKey.getName()).removeItemListener(
          readReadyNotifierRegistrationId);
      readReadyNotifierRegistrationId = null;
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

    context.remove(this);
  }

  @Override
  public void markTemporary() {
    temporary = true;
    context.addTemporaryDataStructure(channelKey);
  }

  @Override
  public boolean isTemporary() {
    return temporary;
  }

  @Override
  public void setAckMode(AckMode ackMode) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public AckMode getAckMode() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private class ReadReadyNotifier implements ItemListener<Object> {

    @Override
    public void itemAdded(ItemEvent<Object> item) {

      synchronized (readReadyMutex) {
        // Clone the list to avoid concurrent modification exception.
        new ArrayList<>(readReadyListeners).stream().forEach(
            l -> l.readReady(QueueChannel.this));
      }
    }

    @Override
    public void itemRemoved(ItemEvent<Object> item) {
      // no op
    }

  }

  private class SendLogic implements Callable<Boolean> {

    private final Message<?> msg;
    private final long timeout;
    private final TimeUnit unit;

    public SendLogic(Message<?> msg, long timeout, TimeUnit unit) {
      this.msg = msg;
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Boolean call() throws Exception {
      BaseQueue<Message<?>> queue = context.getQueue(channelKey.getName(), true);
      return queue.offer(msg, timeout, unit);
    }

  }

  private class ReceiveLogicBusyLoop implements Callable<Message<?>> {

    private final long timeout;
    private final TimeUnit unit;

    public ReceiveLogicBusyLoop(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Message<?> call() throws Exception {

      long remainingTime = unit.toMillis(timeout);
      BaseQueue<Message<?>> queue = context.getQueue(channelKey.getName(), true);
      Message<?> msg;
      boolean interrupted;
      do {
        msg = queue.poll(Math.min(1000, remainingTime), TimeUnit.MILLISECONDS);
        remainingTime = remainingTime - 1000;
        interrupted = Thread.interrupted();

      } while (msg == null && remainingTime > 0 && !interrupted);

      if (interrupted) {
        throw new InterruptedException();
      }

      return msg;
    }
  }

  private class ReceiveLogicBlocking implements Callable<Message<?>> {

    private final long timeout;
    private final TimeUnit unit;

    public ReceiveLogicBlocking(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Message<?> call() throws Exception {
      BaseQueue<Message<?>> queue = context.getQueue(channelKey.getName(), true);
      return queue.poll(timeout, unit);
    }
  }


}
