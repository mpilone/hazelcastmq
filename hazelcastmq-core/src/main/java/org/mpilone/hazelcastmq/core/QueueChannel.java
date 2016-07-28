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
  private final DataStructureKey key;
  private final HazelcastInstance hazelcastInstance;
  private final Object taskMutex;
  private final Object readReadyMutex;
  private final Collection<ReadReadyListener> readReadyListeners;
  private final String readReadyNotifierRegistrationId;

  private FutureTask<Boolean> sendTask;
  private FutureTask<Message<?>> receiveTask;
  private volatile boolean closed;
  private boolean temporary;

  public QueueChannel(DefaultChannelContext context, DataStructureKey key) {

    this.context = context;
    this.key = key;
    this.hazelcastInstance = context.getBroker().getConfig()
        .getHazelcastInstance();
    this.taskMutex = new Object();
    this.readReadyMutex = new Object();
    this.readReadyListeners = new HashSet<>(2);
    this.readReadyNotifierRegistrationId = hazelcastInstance.getQueue(key.
        getName()).addItemListener(new ReadReadyNotifier(), false);
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

      // Use a FutureTask to make tracking the calling thread easier and safe.
      receiveTask = new FutureTask(new ReceiveLogic(timeout, unit));
    }

    Message<?> result;
    try {
      receiveTask.run();
      result = receiveTask.get();
    }
    catch (CancellationException ex) {
      // Cancelled on close.
      result = null;
    }
    catch (ExecutionException ex) {
      if (ex.getCause() instanceof InterruptedException) {
        // Put was interrupted while waiting. Treat it like a cancel.
        Thread.currentThread().interrupt();
        result = null;
      }
      else {
        throw new HazelcastMQException("Error while trying to receive.", ex);
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      result = null;
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

      // Use a FutureTask to make tracking the calling thread easier and safe.
      sendTask = new FutureTask(new SendLogic(msg, timeout, unit));
    }

    boolean result;
    try {
      sendTask.run();
      result = sendTask.get();
    }
    catch (CancellationException ex) {
      // Cancelled on close.
      result = false;
    }
    catch (ExecutionException ex) {
      if (ex.getCause() instanceof InterruptedException) {
        // Put was interrupted while waiting. Treat it like a cancel.
        Thread.currentThread().interrupt();
        result = false;
      }
      else {
        throw new HazelcastMQException("Error while trying to send.", ex);
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      result = false;
    }

    synchronized (taskMutex) {
      sendTask = null;
    }

    return result;
  }

  @Override
  public void addReadReadyListener(ReadReadyListener listener) {
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

    // Stop listening for item events.
    hazelcastInstance.getQueue(key.getName()).removeItemListener(
        readReadyNotifierRegistrationId);

    synchronized (taskMutex) {
      if (sendTask != null) {
        sendTask.cancel(true);
        try {
          sendTask.get();
        }
        catch (InterruptedException | ExecutionException ex) {
          // Ignore because we're closing..
        }
      }

      if (receiveTask != null) {
        receiveTask.cancel(true);
        try {
          receiveTask.get();
        }
        catch (InterruptedException | ExecutionException ex) {
          // Ignore because we're closing.
        }
      }
    }

    context.remove(this);
  }

  @Override
  public void markTemporary() {
    temporary = true;
    context.addTemporaryDataStructure(key);
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
      BaseQueue<Message<?>> queue = context.getQueue(key.getName(), true);
      return queue.offer(msg, timeout, unit);
    }

  }

  private class ReceiveLogic implements Callable<Message<?>> {

    private final long timeout;
    private final TimeUnit unit;

    public ReceiveLogic(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public Message<?> call() throws Exception {
      BaseQueue<Message<?>> queue = context.getQueue(key.getName(), true);
      return queue.poll(timeout, unit);
    }

  }

}
