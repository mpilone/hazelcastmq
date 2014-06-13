package org.mpilone.hazelcastmq.camel;

import org.apache.camel.Message;
import org.apache.camel.RuntimeCamelException;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

/**
 * The configuration for the HazelcastMQ component and endpoint. The
 * configuration allows for customization of various parts of the component and
 * serves as the default values for all endpoints created by the component. All
 * the values can be overridden at the endpoint level using URI parameters in
 * the endpoint URI.
 *
 * @author mpilone
 */
public class HazelcastMQCamelConfig implements Cloneable {

  private HazelcastMQInstance hazelcastMQInstance;
  private MessageConverter messageConverter;
  private int concurrentConsumers;
  private int requestTimeout;
  private int timeToLive;
  private String replyTo;

  /**
   * <p>
   * Constructs the configuration with the following defaults.
   * </p>
   * <ul>
   * <li>messageConverter: {@link DefaultMessageConverter}</li>
   * <li>concurrentConsumers: 1</li>
   * <li>requestTimeout: 20000</li>
   * <li>timeToLive: 0</li>
   * <li>replyTo: null</li>
   * </ul>
   */
  public HazelcastMQCamelConfig() {
    messageConverter = new DefaultMessageConverter();
    concurrentConsumers = 1;
    requestTimeout = 20000;
    timeToLive = 0;
  }

  /**
   * Sets the {@link HazelcastMQInstance} that will be used to access all
   * destinations. A HzMq instance must be set on the configuration before any
   * endpoints are created by the component.
   *
   * @param hazelcastMQInstance the HzMq instance
   */
  public void setHazelcastMQInstance(HazelcastMQInstance hazelcastMQInstance) {
    this.hazelcastMQInstance = hazelcastMQInstance;
  }

  /**
   * Returns the {@link HazelcastMQInstance} that will be used to access all
   * destinations.
   *
   * @return the HzMq instance
   */
  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * Returns the message converter that will be to convert all Camel
   * {@link Message}s to and from {@link HazelcastMQMessage}s.
   *
   * @return the message converter
   */
  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

  /**
   * Sets the message converter that will be to convert all Camel
   * {@link Message}s to and from {@link HazelcastMQMessage}s.
   *
   * @param messageConverter the message converter
   */
  public void setMessageConverter(MessageConverter messageConverter) {
    this.messageConverter = messageConverter;
  }

  /**
   * Returns a copy of this configuration.
   *
   * @return the new copy of the configuration
   */
  public HazelcastMQCamelConfig copy() {
    try {
      HazelcastMQCamelConfig copy = (HazelcastMQCamelConfig) clone();
      return copy;
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeCamelException(e);
    }
  }

  /**
   * <p>
   * Returns the number of concurrent consumers to create on an endpoint. Each
   * consumer will operate in a different thread therefore the endpoint will be
   * able to process one message per consumer concurrently. Unlike the JMS
   * component, dynamic scaling is not supported and the consumer threads are
   * fixed for the life of the endpoint.
   * </p>
   * <p>
   * When consuming from a topic, the concurrent consumers should be 1 or
   * duplicate messages (one for each consumer) would be received from the
   * topic.
   * </p>
   *
   * @return the number of concurrent consumers
   */
  public int getConcurrentConsumers() {
    return concurrentConsumers;
  }

  /**
   * <p>
   * Sets the number of concurrent consumers to create on an endpoint.
   * </p>
   *
   * @param concurrentConsumers the number of concurrent consumers
   *
   * @see #getConcurrentConsumers()
   */
  public void setConcurrentConsumers(int concurrentConsumers) {
    if (concurrentConsumers <= 0) {
      throw new IllegalArgumentException(
          "concurrentConsumers must be greater than 0");
    }

    this.concurrentConsumers = concurrentConsumers;
  }

  /**
   * Returns the time-to-live of the messages (in milliseconds) for all messages
   * produced into the endpoint. A value of 0 indicates that the message should
   * never expire.
   *
   * @return the time-to-live in milliseconds for all messages sent to this
   * endpoint
   */
  public int getTimeToLive() {
    return timeToLive;
  }

  /**
   * Sets the time-to-live of the messages (in milliseconds) for all messages
   * produced into the endpoint. A value of 0 indicates that the message should
   * never expire.
   *
   * @param timeToLive the time-to-live in milliseconds for all messages sent to
   * this endpoint
   */
  public void setTimeToLive(int timeToLive) {
    this.timeToLive = timeToLive;
  }

  /**
   * Returns the timeout (in milliseconds) for waiting for a reply when sending
   * a request/reply (InOut) message. If the timeout expires, an exception is
   * raised and the exchange is marked as failed.
   *
   * @return the timeout in milliseconds
   */
  public int getRequestTimeout() {
    return requestTimeout;
  }

  /**
   * Sets the timeout (in milliseconds) for waiting for a reply when sending a
   * request/reply (InOut) message. If the timeout expires, an exception is
   * raised and the exchange is marked as failed.
   *
   * @param requestTimeout the timeout in milliseconds
   */
  public void setRequestTimeout(int requestTimeout) {
    this.requestTimeout = requestTimeout;
  }

  /**
   * Sets the reply to destination for all request/reply (InOut) messages. The
   * component's producers only support exclusive or temporary reply queues.
   * Because HzMq doesn't support selectors, shared reply queues cannot be used.
   * A unique (and therefore exclusive) reply queue must be configured per
   * endpoint for proper request/reply support. If no reply queue is configured,
   * a temporary queue will be automatically created and used. There is very
   * little overhead with temporary queues in HzMq so a temporary reply queue is
   * a good default option; however an exclusive reply queue may be easier to
   * debug in the event of a problem.
   *
   * @return the reply to destination or null to use a temporary queue
   */
  public String getReplyTo() {
    return replyTo;
  }

  /**
   * Sets the reply to destination for all request/reply (InOut) messages.
   *
   * @param replyTo the reply-to destination or null to use a temporary queue
   *
   * @see #getReplyTo()
   */
  public void setReplyTo(String replyTo) {
    this.replyTo = replyTo;
  }

}
