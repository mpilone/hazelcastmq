package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;

class DefaultHazelcastMQProducer implements HazelcastMQProducer {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private DefaultHazelcastMQContext hazelcastMQContext;
  private HazelcastMQConfig config;
  private String replyTo = null;
  private String correlationId = null;
  private long timeToLive = -1;

  private IdGenerator idGenerator;
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  public DefaultHazelcastMQProducer(DefaultHazelcastMQContext hazelcastMQContext) {
    this.hazelcastMQContext = hazelcastMQContext;
    this.config = hazelcastMQContext.getHazelcastMQInstance().getConfig();

    this.idGenerator = config.getHazelcastInstance().getIdGenerator(
        "hazelcastmqproducer");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQProducer#send(java.lang.String,
   * byte[])
   */
  @Override
  public void send(String destination, byte[] body) {

    // Construct a message
    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setBody(body);

    send(destination, msg);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQProducer#send(java.lang.String,
   * java.lang.String)
   */
  @Override
  public void send(String destination, String body) {
    send(destination, body.getBytes(UTF_8));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQProducer#send(java.lang.String,
   * org.mpilone.hazelcastmq.core.HazelcastMQMessage)
   */
  @Override
  public void send(String destination, HazelcastMQMessage msg) {

    // Apply any producer specific overrides.
    if (correlationId != null) {
      msg.getHeaders().put(Headers.CORRELATION_ID, correlationId);
    }

    if (timeToLive > 0) {
      msg.getHeaders().put(Headers.EXPIRATION,
          String.valueOf(System.currentTimeMillis() + timeToLive));
    }

    if (replyTo != null) {
      msg.getHeaders().put(Headers.REPLY_TO, replyTo);
    }

    msg.setId("hazelcastmq-" + idGenerator.newId());
    msg.setDestination(destination);

    byte[] msgData = config.getMessageConverter().fromMessage(msg);

    BlockingQueue<byte[]> queue = hazelcastMQContext.resolveQueue(destination);
    ITopic<byte[]> topic = null;

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

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQProducer#setReplyTo(java.lang.String
   * )
   */
  @Override
  public HazelcastMQProducer setReplyTo(String destination) {
    this.replyTo = destination;
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQProducer#setCorrelationID(java.
   * lang.String)
   */
  @Override
  public HazelcastMQProducer setCorrelationID(String id) {
    this.correlationId = id;
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQProducer#setTimeToLive(long)
   */
  @Override
  public HazelcastMQProducer setTimeToLive(long millis) {
    this.timeToLive = millis;
    return this;
  }

}
