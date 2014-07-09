package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;

/**
 * The default implementation of a producer.
 *
 * @author mpilone
 */
class DefaultHazelcastMQProducer implements HazelcastMQProducer {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      DefaultHazelcastMQProducer.class);

  /**
   * The context that this producer belongs to.
   */
  private final DefaultHazelcastMQContext hazelcastMQContext;

  /**
   * The instance configuration.
   */
  private final HazelcastMQConfig config;

  /**
   * The ID generator used to generate unique message IDs.
   */
  private final IdGenerator idGenerator;

  /**
   * The destination to use for send operations that don't specify a
   * destination.
   */
  private final String destination;

  /**
   * The default time to live in milliseconds for messages if a time to live
   * isn't explicitly given on the send operation.
   */
  private long timeToLive = 0;

  /**
   * Constructs the producer.
   *
   * @param destination the destination to use for send operations that don't
   * specify a destination
   * @param hazelcastMQContext the context that this producer belongs to
   */
  public DefaultHazelcastMQProducer(String destination,
      DefaultHazelcastMQContext hazelcastMQContext) {
    this.destination = destination;
    this.hazelcastMQContext = hazelcastMQContext;
    this.config = hazelcastMQContext.getHazelcastMQInstance().getConfig();
    
    this.idGenerator = config.getHazelcastInstance().getIdGenerator(
        "hazelcastmqproducer");
  }
  
  @Override
  public void send(HazelcastMQMessage msg) {
    doSend(destination, msg, timeToLive);
  }

  @Override
  public void send(HazelcastMQMessage msg, long timeToLive) {
    if (destination == null) {
      throw new HazelcastMQException(
          "No destination configured for the producer.");
    }

    doSend(destination, msg, timeToLive);
  }

  @Override
  public void send(String body) {
    // Construct a message
    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setBody(body);
    
    send(msg);
  }
  
  @Override
  public void send(byte[] body) {
    // Construct a message
    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setBody(body);
    
    send(msg);
  }
  
  @Override
  public void send(String destination, byte[] body) {
    // Construct a message
    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setBody(body);
    
    send(destination, msg);
  }
  
  @Override
  public void send(String destination, String body) {
    // Construct a message
    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setBody(body);
    
    send(destination, msg);
  }
  
  @Override
  public void send(String destination, HazelcastMQMessage msg) {
    doSend(destination, msg, timeToLive);
  }

  @Override
  public void send(String destination, HazelcastMQMessage msg, long timeToLive) {
    if (this.destination != null) {
      throw new HazelcastMQException(
          "Cannot override producer specified destination.");
    }

    doSend(destination, msg, timeToLive);
  }

  /**
   * Common send implementation that sends the message to the given destination.
   *
   * @param destination the destination to send to
   * @param msg the message to send
   * @param timeToLive the time to live for the message in milliseconds
   */
  protected void doSend(String destination, HazelcastMQMessage msg,
      long timeToLive) {

    // Apply any producer specific overrides.
    if (timeToLive > 0) {
      msg.getHeaders().put(Headers.EXPIRATION,
          String.valueOf(System.currentTimeMillis() + timeToLive));
    }
    
    if (destination == null) {
      throw new HazelcastMQException("Destination is required when "
          + "sending a message.");
    }
    
    msg.setId("hazelcastmq-" + idGenerator.newId());
    msg.setDestination(destination);

    if (log.isDebugEnabled()) {
      log.debug("Producer sending message {}", msg);
    }

    Object msgData = config.getMessageConverter().fromMessage(msg);
    
    BlockingQueue<Object> queue = hazelcastMQContext.resolveQueue(destination);
    ITopic<Object> topic = null;

    // Only resolve the topic if we couldn't resolve it as a queue. This is a
    // minor optimization.
    if (queue == null) {
      topic = hazelcastMQContext.resolveTopic(destination);
    }

    if (queue != null) {
      if (!queue.offer(msgData)) {
        throw new HazelcastMQException(format(
            "Failed to send to queue destination [%s]. Queue is full.",
            destination));
      }
    }
    else if (topic != null) {
      topic.publish(msgData);
    }
    else {
      throw new HazelcastMQException(format(
          "Destination cannot be resolved [%s].", destination));
    }
    
  }
  
  @Override
  public void setTimeToLive(long millis) {
    this.timeToLive = millis;
  }
  
  @Override
  public long getTimeToLive() {
    return timeToLive;
  }
}
