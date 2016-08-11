package org.mpilone.hazelcastmq.camel;

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
 * The consumer supports a configurable number of concurrent consumers. The
 * consumers will be managed by a {@link MultipleThreadPollingMessageDispatcher}
 * and therefore use separate threads. * Unlike the Camel JMS component, this
 * consumer does not scale up or down with load but simply has a fixed number of
 * consumers.
 * </p>
 *
 * @author mpilone
 */
public class CamelConsumer extends DefaultConsumer {

  private final MessageConverter messageConverter;
  private final MultipleThreadPollingMessageDispatcher dispatcher;

  /**
   * Constructs the consumer which will consume messages from the given endpoint
   * destination.
   *
   * @param endpoint the endpoint to consume from
   * @param processor the processor to send incoming messages to
   */
  public CamelConsumer(CamelEndpoint endpoint, Processor processor) {
    super(endpoint, processor);

    CamelConfig config = endpoint.getConfiguration();
    this.messageConverter = config.getMessageConverter();

    dispatcher = new MultipleThreadPollingMessageDispatcher();
    dispatcher.setExecutor(endpoint.getExecutorService());
    dispatcher.setMaxConcurrentPerformers(config.getConcurrentConsumers());
    dispatcher.setPerformer(new ConsumerPerformer(config.getBroker()));
    dispatcher.setBroker(config.getBroker());
    dispatcher.setChannelKey(endpoint.getChannelKey());
  }

  @Override
  public CamelEndpoint getEndpoint() {
    return (CamelEndpoint) super.getEndpoint();
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    dispatcher.start();
  }

  @Override
  protected void doStop() throws Exception {
    dispatcher.stop();

    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    dispatcher.stop();

    super.doShutdown();
  }

  @Override
  protected void doSuspend() throws Exception {
    dispatcher.stop();

    super.doSuspend();
  }

  @Override
  protected void doResume() throws Exception {
    dispatcher.start();

    super.doResume();
  }

  /**
   * A consumer that listens for messages on a destination and dispatches them
   * to a processor. Each consumer manages its own context and consumer
   * instance.
   */
  private class ConsumerPerformer implements MessageDispatcher.Performer {

    private final Broker broker;

    public ConsumerPerformer(Broker broker) {
      this.broker = broker;
    }

    @Override
    public void perform(org.mpilone.hazelcastmq.core.Message<?> msg) {

      DataStructureKey replyToKey = msg.getHeaders().getReplyTo();
      String correlationId = msg.getHeaders().getCorrelationId();

      // Build an exchange.
      org.apache.camel.Message camelMsg = messageConverter.toCamelMessage(msg);
      Exchange camelExchange = getEndpoint().createExchange();

      // Change the pattern to out/in if we have a reply destination
      // and the exchange isn't already out capable.
      if (replyToKey != null && !camelExchange.getPattern().
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

      if (!camelExchange.isFailed() && replyToKey != null
          && camelExchange.getPattern().isOutCapable()) {
        sendReply(correlationId, replyToKey, camelExchange);
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
    private void sendReply(String correlationId, DataStructureKey replyToKey,
        Exchange camelExchange) {

      org.apache.camel.Message camelMsg = camelExchange.hasOut() ?
          camelExchange.getOut() :
          camelExchange.getIn();

      if (camelMsg != null) {
        try {
          org.mpilone.hazelcastmq.core.Message msg = messageConverter.
              fromCamelMessage(camelMsg);

          // Remove the reply-to header because this is the reply.
          // Add the correlation ID so the sender can correlate the reply.
          msg = MessageBuilder.fromMessage(msg).removeHeader(
              MessageHeaders.REPLY_TO).setHeader(MessageHeaders.CORRELATION_ID,
                  correlationId).build();

          // We don't know what the threading model of the dispatcher looks
          // like so we create a new context just for this reply and send it.
          try (ChannelContext context = broker.createChannelContext();
              org.mpilone.hazelcastmq.core.Channel channel = context.
              createChannel(replyToKey)) {
            channel.send(msg);
          }
        }
        catch (Throwable ex) {
          camelExchange.setException(ex);
        }
      }
    }
  }

}
