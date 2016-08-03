package org.mpilone.hazelcastmq.camel;

import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.PollingConsumerSupport;
import org.mpilone.hazelcastmq.camel.MessageConverter;
import org.mpilone.hazelcastmq.core.*;

/**
 * <p>
 * An Apache Camel polling consumer that can consume messages from a HzMq
 * endpoint such as a queue or a topic. To properly release resources, be sure
 * to call {@link #stop()} to dispose of the consumer.
 * </p>
 *
 * @author mpilone
 */
public class CamelPollingConsumer extends PollingConsumerSupport {

  private ChannelContext mqContext;
  private Channel mqChannel;
  private final MessageConverter messageConverter;

  /**
   * Constructs the consumer for the given endpoint.
   *
   * @param endpoint the parent endpoint
   */
  public CamelPollingConsumer(CamelEndpoint endpoint) {
    super(endpoint);

    messageConverter = endpoint.getConfiguration().getMessageConverter();
  }

  @Override
  public CamelEndpoint getEndpoint() {
    return (CamelEndpoint) super.getEndpoint();
  }

  @Override
  protected void doStart() throws Exception {
    if (mqContext == null) {
      mqContext = getEndpoint().getConfiguration().getBroker().
          createChannelContext();
      mqChannel = mqContext.createChannel(getEndpoint().getChannelKey());
    }
  }

  @Override
  protected void doStop() throws Exception {
    if (mqContext != null) {
      mqChannel.close();
      mqContext.close();

      mqChannel = null;
      mqContext = null;
    }
  }

  @Override
  public Exchange receive() {
    org.mpilone.hazelcastmq.core.Message<?> msg = null;

    if (mqChannel != null && !mqChannel.isClosed()) {
      msg = mqChannel.receive();
    }

    return msg == null ? null : createExchange(msg);
  }

  @Override
  public Exchange receiveNoWait() {
    return receive(0);
  }

  @Override
  public Exchange receive(long timeout) {
    org.mpilone.hazelcastmq.core.Message<?> msg = null;

    if (mqChannel != null) {
      msg = mqChannel.receive(timeout, TimeUnit.MILLISECONDS);
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
  private Exchange createExchange(org.mpilone.hazelcastmq.core.Message<?> msg) {
    // Build an exchange.
    Message camelMsg = messageConverter.toCamelMessage(msg);

    Exchange camelExchange = getEndpoint().createExchange();
    camelExchange.setIn(camelMsg);

    return camelExchange;
  }

}
