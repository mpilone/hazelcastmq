package org.mpilone.hazelcastmq.camel;

import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.PollingConsumerSupport;
import org.mpilone.hazelcastmq.core.HazelcastMQConsumer;
import org.mpilone.hazelcastmq.core.HazelcastMQContext;
import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

/**
 * <p>
 * An Apache Camel polling consumer that can consume messages from a HzMq
 * endpoint such as a queue or a topic. To properly release resources, be sure
 * to call {@link #stop()} to dispose of the consumer.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQCamelPollingConsumer extends PollingConsumerSupport {

  private HazelcastMQContext mqContext;
  private HazelcastMQConsumer mqConsumer;
  private final MessageConverter messageConverter;

  /**
   * Constructs the consumer for the given endpoint.
   *
   * @param endpoint the parent endpoint
   */
  public HazelcastMQCamelPollingConsumer(HazelcastMQCamelEndpoint endpoint) {
    super(endpoint);

    messageConverter = endpoint.getConfiguration().getMessageConverter();
  }

  @Override
  public HazelcastMQCamelEndpoint getEndpoint() {
    return (HazelcastMQCamelEndpoint) super.getEndpoint();
  }

  @Override
  protected void doStart() throws Exception {
    if (mqContext == null) {
      mqContext = getEndpoint().getConfiguration().getHazelcastMQInstance().
          createContext();
      mqContext.setAutoStart(true);
      mqConsumer = mqContext.createConsumer(getEndpoint().getDestination());
    }
  }

  @Override
  protected void doStop() throws Exception {
    if (mqContext != null) {
      mqConsumer.close();
      mqContext.close();

      mqConsumer = null;
      mqContext = null;
    }
  }

  @Override
  public Exchange receive() {
    HazelcastMQMessage msg = null;

    if (mqConsumer != null) {
      msg = mqConsumer.receive();
    }

    return msg == null ? null : createExchange(msg);
  }

  @Override
  public Exchange receiveNoWait() {
    HazelcastMQMessage msg = null;

    if (mqConsumer != null) {
      msg = mqConsumer.receiveNoWait();
    }

    return msg == null ? null : createExchange(msg);
  }

  @Override
  public Exchange receive(long timeout) {
    HazelcastMQMessage msg = null;

    if (mqConsumer != null) {
      msg = mqConsumer.receive(timeout, TimeUnit.MILLISECONDS);
    }

    return msg == null ? null : createExchange(msg);
  }

  /**
   * Creates a new exchange for the given message.
   *
   * @param msg the HzMq message to convert to an exchange
   *
   * @return the new exchange
   */
  private Exchange createExchange(HazelcastMQMessage msg) {
    // Build an exchange.
    Message camelMsg = messageConverter.toCamelMessage(msg);

    Exchange camelExchange = getEndpoint().createExchange();
    camelExchange.setIn(camelMsg);

    return camelExchange;
  }

}
