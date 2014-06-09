
package org.mpilone.hazelcastmq.camel;


import org.apache.camel.*;
import org.apache.camel.impl.DefaultProducer;
import org.mpilone.hazelcastmq.core.*;

/**
 *
 * @author mpilone
 */
public class HazelcastMQCamelProducer extends DefaultProducer {
  private final HazelcastMQProducer mqProducer;
  private final HazelcastMQContext mqContext;
  private final MessageConverter messageConverter;

  public HazelcastMQCamelProducer(
      HazelcastMQCamelEndpoint endpoint) {
    super(endpoint);

    HazelcastMQCamelConfig config = endpoint.getConfiguration();

    this.mqContext = config.getHazelcastMQInstance().createContext();
    this.mqProducer = mqContext.createProducer(endpoint.getDestination());
    this.messageConverter = config.getMessageConverter();
  }

  @Override
  public void process(Exchange exchange) throws Exception {
    try {
      Message camelMsg = exchange.getIn();
      mqProducer.send(messageConverter.fromCamelMessage(camelMsg));
    }
    catch (Exception e) {
      exchange.setException(e);
    }
  }

}
