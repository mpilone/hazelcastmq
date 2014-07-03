package org.mpilone.hazelcastmq.camel;

import static java.lang.String.format;
import static org.mpilone.hazelcastmq.camel.HazelcastMQCamelEndpoint.toHazelcastMQDestination;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultProducer;
import org.mpilone.hazelcastmq.camel.MessageConverter;
import org.mpilone.hazelcastmq.core.*;

/**
 * An Apache Camel producer that can produce messages into an HzMq endpoint such
 * as a queue or a topic. The producer supports one way requests (InOnly) and
 * two way request/reply (InOut) patterns.
 *
 * @author mpilone
 */
public class HazelcastMQCamelProducer extends DefaultProducer {

  /**
   * The header in the Camel "in" message that specifies the destination for the
   * specific message being sent. If this value is set in the message, the
   * destination configured for the endpoint will be ignored and the message
   * specific destination will be used instead.
   */
  public static final String CAMEL_HZMQ_DESTINATION = "CamelHzMqDestination";

  private final HazelcastMQCamelConfig config;
  private final MessageConverter messageConverter;
  private final String destination;
  private final ReplyManager replyManager;

  private HazelcastMQContext mqContext;
  private HazelcastMQProducer mqProducer;

  /**
   * Constructs the producer which will produce messages to the given endpoint
   * destination.
   *
   * @param endpoint the endpoint that this instance is producing into
   */
  public HazelcastMQCamelProducer(HazelcastMQCamelEndpoint endpoint) {
    super(endpoint);

    this.config = endpoint.getConfiguration();
    this.messageConverter = config.getMessageConverter();
    this.destination = endpoint.getDestination();

    // Load the reply-to destination from the config first and then allow the
    // reply manager to fallback to temporary queue.
    this.replyManager = new ReplyManager(config.getReplyTo(), config.
        getHazelcastMQInstance());
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    if (mqContext == null) {
      mqContext = config.getHazelcastMQInstance().createContext();
      mqProducer = mqContext.createProducer();
    }

    replyManager.start();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();

    if (mqContext != null) {
      mqProducer = null;
      mqContext.stop();
      mqContext = null;
    }

    replyManager.stop();
  }

  @Override
  protected void doShutdown() throws Exception {
    super.doShutdown();

    replyManager.shutdown();
  }

  @Override
  public void process(Exchange exchange) throws Exception {

    Message camelMsg = exchange.getIn();

    // Handle a message specific destination if set in the headers; otherwise
    // use the destination configured in the endpoint.
    String msgDest = camelMsg.getHeader(CAMEL_HZMQ_DESTINATION) != null ?
        toHazelcastMQDestination(camelMsg.getHeader(CAMEL_HZMQ_DESTINATION).
            toString()) : destination;

    HazelcastMQMessage msg = messageConverter.fromCamelMessage(camelMsg);

    try {
      if (exchange.getPattern().isOutCapable()) {
        processInOut(msgDest, msg, exchange, mqProducer);
      }
      else {
        processInOnly(msgDest, msg, exchange, mqProducer);
      }
    }
    catch (Throwable e) {
      exchange.setException(e);
    }
  }

  /**
   * Processes an InOnly exchange by converting the message in the exchange and
   * sending it to the configured endpoint destination.
   *
   * @param destination the destination to send the message to
   * @param msg the HzMq message to be sent
   * @param exchange the exchange to process
   * @param mqProducer the producer to use for sending
   *
   * @throws Exception if there is an error producing the message to the
   * exchange
   */
  private void processInOnly(String destination, HazelcastMQMessage msg,
      Exchange exchange, HazelcastMQProducer mqProducer)
      throws Exception {
    int timeToLive = config.getTimeToLive();
    
    mqProducer.send(destination, msg, timeToLive);
  }

  /**
   * <p>
   * Processes an InOut exchange by converting the message in the exchange and
   * sending it to the configured endpoint destination. A replyTo destination
   * will be set on the produced message as well as a correlation ID if one
   * isn't already present on the message. This method will block for the
   * configured reply timeout or until a reply is received, which ever comes
   * first.
   * </p>
   * <p>
   * The producer only supports exclusive or temporary reply queues. Because
   * HzMq doesn't support selectors, shared reply queues cannot be used. An
   * unique (and therefore exclusive) reply queue must be configured per
   * endpoint for proper request/reply support. If no reply queue is configured,
   * a temporary queue will be automatically created and used. There is very
   * little overhead with temporary queues in HzMq so a temporary reply queue is
   * a good default option; however an exclusive reply queue may be easier to
   * debug in the event of a problem.
   * </p>
   *
   * @param destination the destination to send the message to
   * @param msg the HzMq message to be sent
   * @param exchange the exchange to process
   * @param mqProducer the producer to use for sending
   *
   * @throws Exception if there is an error producing the message to the
   * exchange
   */
  private void processInOut(String destination, HazelcastMQMessage msg,
      Exchange exchange, HazelcastMQProducer mqProducer)
      throws Exception {

    int timeToLive = config.getTimeToLive();
    int requestTimeout = config.getRequestTimeout();

    BlockingQueue<HazelcastMQMessage> replyQueue = new ArrayBlockingQueue<>(1);

    String replyToDestination = replyManager.getReplyToDestination();

    msg.setReplyTo(replyToDestination);
    if (msg.getCorrelationId() == null) {
      msg.setCorrelationId("CamelHzMq-" + UUID.randomUUID().toString());
    }

    // Add the pending reply before sending the message to prevent a race
    // condition if the reply arrived before we're ready.
    replyManager.addPendingReply(msg.getCorrelationId(), requestTimeout,
        replyQueue);

    // Send the message.
    mqProducer.send(destination, msg, timeToLive);

    // Wait for the reply.
    HazelcastMQMessage replyMsg = replyQueue.poll(requestTimeout,
        TimeUnit.MILLISECONDS);

    if (replyMsg == null) {
      throw new RuntimeCamelException(format(
          "No reply received for correlation ID %s on reply to "
          + "destination %s after %d milliseconds.",
          msg.getCorrelationId(), replyToDestination, requestTimeout));
    }
    else if (replyMsg == ReplyManager.SENTINAL) {
      // This is expected if we're shutting down. No reply will be processed.
    }
    else {
      exchange.setOut(messageConverter.toCamelMessage(replyMsg));
    }
  }

  /**
   * A reply manager that can listener for replies on a given destination. The
   * replies are correlated with producers pending a reply via the message
   * correlation ID.
   */
  private static class ReplyManager implements HazelcastMQMessageListener,
      ShutdownableService {

    private final HazelcastMQContext mqContext;
    private final HazelcastMQConsumer mqConsumer;
    private final Map<String, PendingReply> pendingReplyMap;
    private final AtomicLong nextExpiration;
    private final String replyToDestination;
    public final static HazelcastMQMessage SENTINAL = new HazelcastMQMessage();

    /**
     * Constructs the reply manager which will listen for replies on the given
     * destination. If the destination is null, a temporary reply queue will be
     * created and managed by the reply manager.
     *
     * @param replyToDest the reply to destination or null
     * @param mqInstance the HzMq instance brokering all replies
     */
    public ReplyManager(String replyToDest, HazelcastMQInstance mqInstance) {

      this.pendingReplyMap = new ConcurrentHashMap<>();
      this.nextExpiration = new AtomicLong(Long.MAX_VALUE);

      this.mqContext = mqInstance.createContext();
      mqContext.setAutoStart(false);

      // If we don't have an exclusive reply queue, create a temporary queue.
      this.replyToDestination = replyToDest == null ?
          mqContext.createTemporaryQueue() : replyToDest;

      this.mqConsumer = mqContext.createConsumer(this.replyToDestination);
      mqConsumer.setMessageListener(this);
    }

    @Override
    public void start() {
      mqContext.start();
    }

    @Override
    public void stop() {
      mqContext.stop();
    }

    @Override
    public void shutdown() {
      mqConsumer.close();
      mqContext.close();

      // Send a sentinal value to all pending producers.
      for (Iterator<PendingReply> iter = pendingReplyMap.values().iterator();
          iter.hasNext();) {

        PendingReply pendingReply = iter.next();
        iter.remove();

        pendingReply.getReplyQueue().offer(SENTINAL);
      }
    }

    /**
     * Returns the replyTo destination that should be used for all messages to
     * be handled by this reply manager instance.
     *
     * @return the replyTo destination
     */
    public String getReplyToDestination() {
      return replyToDestination;
    }

    /**
     * Adds a pending reply to be tracked for the given correlation ID.
     *
     * @param correlationId the ID of the expected reply
     * @param timeout the number of milliseconds to wait for the reply before
     * dropping the pending reply
     * @param replyQueue the queue to write the reply to once received
     */
    public void addPendingReply(String correlationId, long timeout,
        Queue<HazelcastMQMessage> replyQueue) {
      long expiration = System.currentTimeMillis() + timeout;

      PendingReply pendingReply = new PendingReply(correlationId,
          expiration, replyQueue);

      pendingReplyMap.put(correlationId, pendingReply);
      setIfLess(nextExpiration, expiration);
    }

    @Override
    public void onMessage(HazelcastMQMessage msg) {

      long now = System.currentTimeMillis();
      PendingReply pendingReply = null;

      // Cleanup expired pending replies.
      if (nextExpiration.get() <= now) {
        nextExpiration.set(Long.MAX_VALUE);

        for (Iterator<PendingReply> iter = pendingReplyMap.values().iterator();
            iter.hasNext();) {
          pendingReply = iter.next();

          if (pendingReply.getExpiration() <= now) {
            iter.remove();
          }
          else {
            setIfLess(nextExpiration, pendingReply.getExpiration());
          }
        }
      }

      // Process the incoming reply.
      String correlationId = msg.getCorrelationId();

      if (correlationId != null) {
        pendingReply = pendingReplyMap.remove(correlationId);
      }

      if (pendingReply != null) {
        pendingReply.getReplyQueue().offer(msg);
      }
      else {
        // TODO: log this
      }
    }

    /**
     * Sets the value of the current long if the given update value is less than
     * the current value.
     *
     * @param current the current value
     * @param update the potential update value
     *
     * @return true if the value is set
     */
    private boolean setIfLess(AtomicLong current, long update) {
      while (true) {
        long cur = current.get();

        if (update < cur) {
          if (current.compareAndSet(cur, update)) {
            return true;
          }
        }
        else {
          return false;
        }
      }
    }
  }

  /**
   * A pending reply for a producer. The pending reply is correlated via the
   * correlation ID and will expire at some point in time.
   */
  private static class PendingReply {

    private final String correlationId;
    private final long expiration;
    private final Queue<HazelcastMQMessage> replyQueue;

    /**
     * Constructs the pending reply.
     *
     * @param correlationId the correlation ID used to correlate this reply with
     * incoming reply messages
     * @param expiration the expiration date of this pending reply in unix time
     * milliseconds
     * @param replyQueue the reply queue to offer the reply message to when it
     * arrives
     */
    public PendingReply(String correlationId, long expiration,
        Queue<HazelcastMQMessage> replyQueue) {
      this.correlationId = correlationId;
      this.expiration = expiration;
      this.replyQueue = replyQueue;
    }

    /**
     * Returns the correlation ID used to correlate this reply with incoming
     * reply messages.
     *
     * @return the correlation ID
     */
    public String getCorrelationId() {
      return correlationId;
    }

    /**
     * Returns the expiration date of this pending reply in unix time
     * milliseconds.
     *
     * @return the expiration date
     */
    public long getExpiration() {
      return expiration;
    }

    /**
     * Returns the reply queue to offer the reply message to when it arrives.
     *
     * @return the reply queue
     */
    public Queue<HazelcastMQMessage> getReplyQueue() {
      return replyQueue;
    }
  }
}
