
package org.mpilone.hazelcastmq.camel;


import org.apache.camel.*;
import org.apache.camel.impl.*;
import org.mpilone.hazelcastmq.camel.MessageConverter;
import org.mpilone.hazelcastmq.core.*;

/**
 *
 * @author mpilone
 */
public class HazelcastMQCamelConsumer extends DefaultConsumer implements
    HazelcastMQMessageListener {

  private final HazelcastMQContext mqContext;
  private final HazelcastMQConsumer mqConsumer;
  private final MessageConverter messageConverter;

  public HazelcastMQCamelConsumer(HazelcastMQCamelEndpoint endpoint,
      Processor processor) {
    super(endpoint, processor);

    HazelcastMQCamelConfig config = endpoint.getConfiguration();

    this.mqContext = config.getHazelcastMQInstance().createContext();
    this.mqContext.setAutoStart(false);

    this.mqConsumer = mqContext.createConsumer(endpoint.getDestination());
    this.mqConsumer.setMessageListener(this);
    this.messageConverter = config.getMessageConverter();
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    mqContext.start();
  }

  @Override
  protected void doStop() throws Exception {
    mqContext.stop();

    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    mqConsumer.close();
    mqContext.close();

    super.doShutdown();
  }

  @Override
  public void onMessage(HazelcastMQMessage msg) {

    Message camelMsg = messageConverter.toCamelMessage(msg);
    Exchange camelExchange = getEndpoint().createExchange();
    camelExchange.setIn(camelMsg);

    try {
      getProcessor().process(camelExchange);
    }
    catch (Throwable e) {
      camelExchange.setException(e);
    }

    if (camelExchange.getException() != null) {
      getExceptionHandler().handleException("Error processing exchange.",
          camelExchange, camelExchange.getException());
    }
  }


}
