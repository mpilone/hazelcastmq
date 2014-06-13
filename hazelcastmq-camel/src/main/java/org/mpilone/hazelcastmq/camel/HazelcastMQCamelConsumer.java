package org.mpilone.hazelcastmq.camel;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.*;
import org.apache.camel.impl.*;
import org.mpilone.hazelcastmq.camel.MessageConverter;
import org.mpilone.hazelcastmq.core.*;

/**
 * <p>
 * An Apache Camel consumer that can consume messages from a HzMq endpoint such
 * as a queue or a topic. The consumer supports one way requests (InOnly) and
 * two way request/reply (InOut) patterns. The exchange pattern will be
 * automatically set to InOut if the incoming message has a reply destination
 * configured.
 * </p>
 * <p>
 * The consumer supports a configurable number of concurrent consumers. Each
 * concurrent consumer will be managed in a separate {@link HazelcastMQContext}
 * and therefore a separate thread. If consuming from a topic, it is important
 * to only have one concurrent consumer or duplicate messages will be received.
 * Unlike the Camel JMS component, this consumer does not scale up or down with
 * load but simply has a fixed number of consumers.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQCamelConsumer extends DefaultConsumer {

  private final MessageConverter messageConverter;
  private final List<SingleThreadedConsumer> consumers;

  /**
   * Constructs the consumer which will consume messages from the given endpoint
   * destination.
   *
   * @param endpoint the endpoint to consume from
   * @param processor the processor to send incoming messages to
   */
  public HazelcastMQCamelConsumer(HazelcastMQCamelEndpoint endpoint,
      Processor processor) {
    super(endpoint, processor);

    HazelcastMQCamelConfig config = endpoint.getConfiguration();
    this.messageConverter = config.getMessageConverter();
    this.consumers = new ArrayList<>();

    // Create the number of consumers requested.
    int concurrentConsumers = config.getConcurrentConsumers();
    for (int i = 0; i < concurrentConsumers; ++i) {
      this.consumers.add(new SingleThreadedConsumer(config, endpoint));
    }
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    for (SingleThreadedConsumer consumer : consumers) {
      consumer.start();
    }
  }

  @Override
  protected void doStop() throws Exception {
    for (SingleThreadedConsumer consumer : consumers) {
      consumer.stop();
    }

    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    for (SingleThreadedConsumer consumer : consumers) {
      consumer.shutdown();
    }

    super.doShutdown();
  }

  /**
   * A consumer that listens for messages on a destination and
   * dispatches them to a processor. Each consumer manages its own
   * context and consumer instance.
   */
  private class SingleThreadedConsumer implements HazelcastMQMessageListener,
      ShutdownableService {

    private final HazelcastMQContext mqContext;
    private final HazelcastMQConsumer mqConsumer;

    /**
     * Constructs the consumer. The consumer will not start listening for
     * messages until it is started.
     *
     * @param config the component configuration
     * @param endpoint the endpoint that the consumer belongs to
     */
    public SingleThreadedConsumer(HazelcastMQCamelConfig config,
        HazelcastMQCamelEndpoint endpoint) {
      this.mqContext = config.getHazelcastMQInstance().createContext();
      this.mqContext.setAutoStart(false);

      this.mqConsumer = mqContext.createConsumer(endpoint.getDestination());
      this.mqConsumer.setMessageListener(this);
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
    }

    @Override
    public void onMessage(HazelcastMQMessage msg) {

      String replyToDestination = msg.getReplyTo();
      String correlationId = msg.getCorrelationId();

      // Build an exchange.
      Message camelMsg = messageConverter.toCamelMessage(msg);
      Exchange camelExchange = getEndpoint().createExchange();

      // Change the pattern to out/in if we have a reply destination
      // and the exchange isn't already out capable.
      if (replyToDestination != null && !camelExchange.getPattern().
          isOutCapable()) {
        camelExchange.setPattern(ExchangePattern.OutIn);
      }

      camelExchange.setIn(camelMsg);

      try {
        getProcessor().process(camelExchange);
      }
      catch (Throwable e) {
        camelExchange.setException(e);
      }

      if (!camelExchange.isFailed() && replyToDestination != null
          && camelExchange.getPattern().isOutCapable()) {
        sendReply(correlationId, replyToDestination, camelExchange);
      }

      if (camelExchange.isFailed()) {
        getExceptionHandler().handleException("Error processing exchange.",
            camelExchange, camelExchange.getException());
      }
    }

    /**
     * Sends a reply message if the exchange has a valid message in the out or
     * in fields (in that order).
     *
     * @param correlationId the optional correlation ID to set on the out going
     * message
     * @param replyToDestination the reply to destination
     * @param camelExchange the exchange containing the message to send as the
     * reply
     */
    private void sendReply(String correlationId, String replyToDestination,
        Exchange camelExchange) {

      Message camelMsg = camelExchange.hasOut() ? camelExchange.getOut() :
          camelExchange.getIn();

      if (camelMsg != null) {
        try {
          HazelcastMQMessage msg = messageConverter.fromCamelMessage(camelMsg);
          msg.getHeaders().remove(org.mpilone.hazelcastmq.core.Headers.REPLY_TO);
          msg.setDestination(replyToDestination);
          msg.setCorrelationId(correlationId);

          mqContext.createProducer(replyToDestination).send(msg);
        }
        catch (Throwable ex) {
          camelExchange.setException(ex);
        }
      }
    }
  }

}
