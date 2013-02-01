package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;

/**
 * A JMS message consumer that can consume from a HazelcastMQ queue or topic.
 * 
 * @author mpilone
 */
public abstract class HazelcastMQMessageConsumer implements MessageConsumer {

  /**
   * The parent session.
   */
  protected HazelcastMQSession session;

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The JMS destination from which to consume.
   */
  protected Destination destination;

  /**
   * The Hazelcast instance from which to consume.
   */
  protected HazelcastInstance hazelcast;

  /**
   * The message listener that will be notified of new messages as they arrive.
   */
  protected MessageListener messageListener;

  /**
   * The JMS message selector. Currently not used or supported.
   */
  private String messageSelector;

  /**
   * The message marshaller used to marshal messages in and out of HazelcastMQ.
   */
  protected MessageConverter messageMarshaller;

  /**
   * The flag which indicates if this consumer has been closed.
   */
  protected boolean closed = false;

  /**
   * The flag which indicates if this consumer has been started yet.
   */
  protected boolean started = false;

  /**
   * The guard/mutex around stopping the consumer. The JMS specification states
   * that when a connection is stopped, it must block until all consumers are no
   * longer blocking on receive or dispatching to a message listener. That
   * behavior is achieved by a mutex on stopping and receiving messages.
   */
  protected final Object STOP_GUARD = new Object();

  /**
   * Constructs the consumer which will consume from the given destination.
   * 
   * @param session
   *          the parent session
   * @param destination
   *          the destination from which to consume
   * @throws JMSException
   */
  public HazelcastMQMessageConsumer(HazelcastMQSession session,
      Destination destination) throws JMSException {
    this(session, destination, null);
  }

  /**
   * Constructs the consumer which will consume from the given destination.
   * 
   * @param session
   *          the parent session
   * @param destination
   *          the destination from which to consume
   * @param messageSelector
   *          the message selector to filter incoming messages (currently not
   *          supported)
   * @throws JMSException
   */
  public HazelcastMQMessageConsumer(HazelcastMQSession session,
      Destination destination, String messageSelector) throws JMSException {
    this.session = session;
    this.destination = destination;
    this.messageSelector = messageSelector;

    this.messageMarshaller = this.session.getConfig().getMessageConverter();
    this.hazelcast = this.session.getHazelcast();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    stop();
    this.closed = true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#getMessageListener()
   */
  @Override
  public MessageListener getMessageListener() throws JMSException {
    return messageListener;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#getMessageSelector()
   */
  @Override
  public String getMessageSelector() throws JMSException {
    return messageSelector;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
   */
  @Override
  public void setMessageListener(MessageListener messageListener)
      throws JMSException {
    this.messageListener = messageListener;
  }

  /**
   * Receives a message from the given blocking queue, waiting up to the given
   * timeout value.
   * 
   * @param queue
   *          the queue from which to consume
   * @param timeout
   *          greater than 0 to wait the given number of milliseconds, 0 for
   *          unlimited blocking wait, less than zero for no wait
   * @return the message received or null if no message was received
   * @throws JMSException
   */
  protected Message receive(BlockingQueue<byte[]> queue, long timeout)
      throws JMSException {

    // Figure out the receive strategy.
    ReceiveStrategy receiver = null;
    if (timeout < 0) {
      receiver = new IndefiniteBlockingReceive();
    }
    else {
      receiver = new TimedBlockingReceive(timeout);
    }

    Message msg = null;
    do {
      synchronized (STOP_GUARD) {
        try {
          byte[] msgData = receiver.receive(queue);

          // Check that we got message data
          if (msgData != null) {

            // Convert the message data back into a JMS message
            msg = messageMarshaller.toMessage(msgData);

            // Switch Bytes messages to read mode
            if (msg instanceof BytesMessage) {
              ((BytesMessage) msg).reset();
            }

            // Check for message expiration
            long expirationTime = msg.getJMSExpiration();

            if (expirationTime != 0
                && expirationTime <= System.currentTimeMillis()) {
              log.info(format("Dropping message [%s] because it has expired.",
                  msg.getJMSMessageID()));
              msg = null;
            }
          }
        }
        catch (InterruptedException ex) {
          throw new JMSException("Error receiving message: " + ex.getMessage());
        }
        catch (IOException ex) {
          throw new JMSException("Error receiving message: " + ex.getMessage());
        }
      }
    }
    while (msg == null && receiver.isRetryable());

    return msg;
  }

  /**
   * A strategy for receiving messages from a queue.
   * 
   * @author mpilone
   */
  private interface ReceiveStrategy {
    /**
     * Receives a single message from the queue.
     * 
     * @param queue
     *          the queue from which to receive
     * @return the message received or null if no message was available
     * @throws InterruptedException
     */
    public byte[] receive(BlockingQueue<byte[]> queue)
        throws InterruptedException;

    /**
     * Return true if the strategy supports retrying. The receive method will be
     * called until a valid message is received or until this method returns
     * false.
     * 
     * @return true if the receive can be retried, false otherwise
     */
    public boolean isRetryable();
  }

  /**
   * A receive strategy that will wait for a message indefinitely. The receive
   * method will only return when a message has arrived or the consumer is
   * closed.
   * 
   * @author mpilone
   */
  private class IndefiniteBlockingReceive implements ReceiveStrategy {
    /*
     * (non-Javadoc)
     * 
     * @see org.mpilone.hazelcastmq.HazelcastMQMessageConsumer.ReceiveStrategy#
     * isRetryable()
     */
    @Override
    public boolean isRetryable() {
      return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.HazelcastMQMessageConsumer.ReceiveStrategy#receive
     * (java.util.concurrent.BlockingQueue)
     */
    @Override
    public byte[] receive(BlockingQueue<byte[]> queue)
        throws InterruptedException {
      // If this consumer has been concurrently closed, abort the blocking
      // receive.
      while (!HazelcastMQMessageConsumer.this.closed) {

        // If the connection isn't started yet, block indefinitely.
        if (!session.getConnection().isStarted()) {
          Thread.sleep(500);
        }
        else {
          return queue.poll(500, TimeUnit.MILLISECONDS);
        }
      }

      return null;
    }
  }

  /**
   * A receive strategy that blocks for a specific amount of time before
   * returning.
   * 
   * @author mpilone
   */
  private class TimedBlockingReceive implements ReceiveStrategy {
    private long timeout = 0;

    public TimedBlockingReceive(long timeout) {
      this.timeout = timeout;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.mpilone.hazelcastmq.HazelcastMQMessageConsumer.ReceiveStrategy#
     * isRetryable()
     */
    @Override
    public boolean isRetryable() {
      return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.HazelcastMQMessageConsumer.ReceiveStrategy#receive
     * (java.util.concurrent.BlockingQueue)
     */
    @Override
    public byte[] receive(BlockingQueue<byte[]> queue)
        throws InterruptedException {

      if (!started) {
        // Is this an error or should just ignore it? Should we still honor the
        // timeout if the connection isn't started?
        if (timeout > 0) {
          Thread.sleep(timeout);
          return null;
        }
      }

      if (timeout == 0) {
        return queue.poll();
      }
      else {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Performs an atomic {@link #receiveNoWait()} and dispatch to the listener
   * within the STOP_GUARD which ensures that the consumer cannot be stopped
   * until the listener has completed processing of the message.
   * 
   * @param listener
   *          the listener to which to dispatch
   * @return true if a message was dispatched, false otherwise
   * @throws JMSException
   */
  protected boolean receiveAndDispatch(BlockingQueue<byte[]> queue,
      MessageListener listener) throws JMSException {
    synchronized (STOP_GUARD) {
      Message msg = receive(queue, 0);

      if (msg != null) {
        listener.onMessage(msg);
        return true;
      }
      else {
        return false;
      }
    }
  }

  /**
   * Starts this session. This method must be called by the session when it is
   * started.
   */
  void start() {
    started = true;
  }

  /**
   * Stops this session. This method must be called by the session when it is
   * stopped.
   */
  void stop() {
    synchronized (STOP_GUARD) {
      started = false;
    }
  }

}
