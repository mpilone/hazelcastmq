package org.mpilone.hazelcastmq.camel;

import static java.lang.String.format;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultProducer;
import org.mpilone.hazelcastmq.camel.MessageConverter;
import org.mpilone.hazelcastmq.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.collection.impl.queue.QueueService;

/**
 * An Apache Camel producer that can produce messages into an HzMq endpoint such
 * as a queue or a topic. The producer supports one way requests (InOnly) and
 * two way request/reply (InOut) patterns.
 *
 * @author mpilone
 */
public class CamelProducer extends DefaultProducer {

  /**
   * The log for this class.
   */
  private static final Logger log = LoggerFactory.getLogger(CamelProducer.class);

  /**
   * The header in the Camel "in" message that specifies the destination for the
   * specific message being sent. If this value is set in the message, the
   * destination configured for the endpoint will be ignored and the message
   * specific destination will be used instead.
   */
  public static final String CAMEL_HZMQ_DESTINATION = "CamelHzMqDestination";

  private final CamelConfig config;
  private final MessageConverter messageConverter;
  private final DataStructureKey channelKey;
  private final ReplyManager replyManager;
  private final Clock clock = Clock.systemUTC();

  private ChannelContext mqContext;
  private org.mpilone.hazelcastmq.core.Channel mqChannel;

  /**
   * Constructs the producer which will produce messages to the given endpoint
   * destination.
   *
   * @param endpoint the endpoint that this instance is producing into
   */
  public CamelProducer(CamelEndpoint endpoint) {
    super(endpoint);

    this.config = endpoint.getConfiguration();
    this.messageConverter = config.getMessageConverter();
    this.channelKey = endpoint.getChannelKey();

    // Load the reply-to destination from the config first and then allow the
    // reply manager to fallback to temporary queue.
    this.replyManager =
        new ReplyManager(config.getReplyTo(), config.getBroker(), endpoint.
            getExecutorService());
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    if (mqContext == null) {
      mqContext = config.getBroker().createChannelContext();
      mqChannel = mqContext.createChannel(channelKey);
    }

    replyManager.start();
  }

  @Override
  protected void doStop() throws Exception {
    replyManager.stop();

    if (mqContext != null) {
      mqChannel.close();
      mqContext.close();

      mqChannel = null;
      mqContext = null;
    }

    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    replyManager.shutdown();

    super.doShutdown();
  }

  @Override
  public void process(Exchange exchange) throws Exception {

    org.apache.camel.Message camelMsg = exchange.getIn();

    // Handle a message specific destination if set in the headers; otherwise
    // use the destination configured in the endpoint.
    String altDestination = (String) camelMsg.getHeader(CAMEL_HZMQ_DESTINATION);
    org.mpilone.hazelcastmq.core.Channel channel = mqChannel;
    if (altDestination != null) {
      channel = mqContext.createChannel(DataStructureKey.fromString(
          altDestination));
    }

    org.mpilone.hazelcastmq.core.Message<?> msg =
        messageConverter.fromCamelMessage(camelMsg);

    try {
      if (exchange.getPattern().isOutCapable()) {
        processInOut(msg, exchange, channel);
      }
      else {
        processInOnly(msg, exchange, channel);
      }
    }
    catch (Throwable e) {
      exchange.setException(e);
    }
    finally {
      // Close the channel if we were using an alternative destination
      // rather than the one configured in the endpoint.
      if (altDestination != null) {
        channel.close();
      }
    }
  }

  /**
   * Processes an InOnly exchange by converting the message in the exchange and
   * sending it to the configured endpoint destination.
   *
   * @param msg the HzMq message to be sent
   * @param exchange the exchange to process
   * @param channel the channel to send to
   *
   * @throws Exception if there is an error producing the message to the
   * exchange
   */
  private void processInOnly(org.mpilone.hazelcastmq.core.Message<?> msg,
      Exchange exchange, org.mpilone.hazelcastmq.core.Channel channel)
      throws Exception {

    int timeToLive = config.getTimeToLive();
    if (timeToLive > 0) {
      Instant expiration = Instant.now(clock).
          plus(timeToLive, ChronoUnit.MILLIS);

      msg = MessageBuilder.fromMessage(msg).setHeaderIfAbsent(
          MessageHeaders.EXPIRATION, expiration.toEpochMilli()).build();
    }

    channel.send(msg);
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
   * @param msg the HzMq message to be sent
   * @param exchange the exchange to process
   * @param channel the channel to send the message to
   *
   * @throws Exception if there is an error producing the message to the
   * exchange
   */
  private void processInOut(org.mpilone.hazelcastmq.core.Message<?> msg,
      Exchange exchange, org.mpilone.hazelcastmq.core.Channel channel)
      throws Exception {

    int timeToLive = config.getTimeToLive();
    int requestTimeout = config.getRequestTimeout();
    BlockingQueue<org.mpilone.hazelcastmq.core.Message<?>> replyQueue =
        new ArrayBlockingQueue<>(1);
    DataStructureKey replyToKey = replyManager.getReplyToDestination();
    String correlationId = msg.getHeaders().getCorrelationId();

    MessageBuilder<?> builder = MessageBuilder.fromMessage(msg).setHeader(
        MessageHeaders.REPLY_TO, replyToKey);

    if (timeToLive > 0) {
      Instant expiration = Instant.now(clock).
          plus(timeToLive, ChronoUnit.MILLIS);

      builder.setHeaderIfAbsent(MessageHeaders.EXPIRATION, expiration.
          toEpochMilli());
    }

    if (correlationId == null) {
      correlationId = UUID.randomUUID().toString();
      builder.setHeader(MessageHeaders.CORRELATION_ID, correlationId);
    }

    msg = builder.build();

    // Add the pending reply before sending the message to prevent a race
    // condition if the reply arrived before we're ready.
    replyManager.addPendingReply(msg.getHeaders().getCorrelationId(),
        requestTimeout, replyQueue);

    // Send the message.
    channel.send(msg);

    // Wait for the reply.
    org.mpilone.hazelcastmq.core.Message<?> replyMsg = replyQueue.poll(
        requestTimeout, TimeUnit.MILLISECONDS);

    if (replyMsg == null) {
      throw new RuntimeCamelException(format(
          "No reply received for correlation ID %s on reply-to "
          + "channel %s after %d milliseconds.",
          msg.getHeaders().getCorrelationId(), replyToKey, requestTimeout));
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
  private static class ReplyManager implements ShutdownableService,
      MessageDispatcher.Performer {

    private final ChannelContext mqContext;
    private final SingleThreadPollingMessageDispatcher messageDispatcher;
    private final Map<String, PendingReply> pendingReplyMap;
    private final AtomicLong nextExpiration;
    private final DataStructureKey replyToKey;
    public final static org.mpilone.hazelcastmq.core.Message<String> SENTINAL =
        MessageBuilder.createMessage("SENTINAL", null);

    /**
     * Constructs the reply manager which will listen for replies on the given
     * destination. If the destination is null, a temporary reply queue will be
     * created and managed by the reply manager.
     *
     * @param replyToDestination the reply-to camel destination or null
     * @param mqInstance the HzMq broker for all replies
     */
    public ReplyManager(String replyToDestination, Broker broker,
        ExecutorService executorService) {

      this.pendingReplyMap = new ConcurrentHashMap<>();
      this.nextExpiration = new AtomicLong(Long.MAX_VALUE);
      this.mqContext = broker.createChannelContext();

      // If we don't have an exclusive reply queue, create a temporary queue.
      if (replyToDestination == null) {
        this.replyToKey = new DataStructureKey("CamelHzMq-ReplyTo-" + UUID.
            randomUUID().toString(), QueueService.SERVICE_NAME);

        // Mark the channel as temporary so it will be destroyed when
        // we close the context.
        try (org.mpilone.hazelcastmq.core.Channel channel = mqContext.
            createChannel(replyToKey)) {
          channel.markTemporary();
        }
      }
      else {
        this.replyToKey = DataStructureKey.fromString(replyToDestination);
      }

      this.messageDispatcher = new SingleThreadPollingMessageDispatcher();
      messageDispatcher.setPerformer(this);
      messageDispatcher.setBroker(broker);
      messageDispatcher.setChannelKey(replyToKey);
      messageDispatcher.setExecutor(executorService);
    }

    @Override
    public void start() {
      messageDispatcher.start();
    }

    @Override
    public void stop() {
      messageDispatcher.stop();

        // Send a sentinal value to all pending producers.
        for (Iterator<PendingReply> iter = pendingReplyMap.values().iterator();
            iter.hasNext();) {

          PendingReply pendingReply = iter.next();
          iter.remove();

          pendingReply.getReplyQueue().offer(SENTINAL);
        }
    }

    @Override
    public void shutdown() throws Exception {
      mqContext.close();
    }

    /**
     * Returns the replyTo destination that should be used for all messages to
     * be handled by this reply manager instance.
     *
     * @return the replyTo destination
     */
    public DataStructureKey getReplyToDestination() {
      return replyToKey;
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
        Queue<org.mpilone.hazelcastmq.core.Message<?>> replyQueue) {
      long expiration = System.currentTimeMillis() + timeout;

      PendingReply pendingReply = new PendingReply(correlationId,
          expiration, replyQueue);

      pendingReplyMap.put(correlationId, pendingReply);
      setIfLess(nextExpiration, expiration);
    }

    @Override
    public void perform(org.mpilone.hazelcastmq.core.Message<?> msg) {

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
      String correlationId = msg.getHeaders().getCorrelationId();

      if (correlationId != null) {
        pendingReply = pendingReplyMap.remove(correlationId);
      }

      if (pendingReply != null) {
        pendingReply.getReplyQueue().offer(msg);
      }
      else {
        log.info("Received reply for unknown correlation ID {}. "
            + "The reply will be discarded.", correlationId);
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
    private final Queue<org.mpilone.hazelcastmq.core.Message<?>> replyQueue;

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
        Queue<org.mpilone.hazelcastmq.core.Message<?>> replyQueue) {
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
    public Queue<org.mpilone.hazelcastmq.core.Message<?>> getReplyQueue() {
      return replyQueue;
    }
  }
}
