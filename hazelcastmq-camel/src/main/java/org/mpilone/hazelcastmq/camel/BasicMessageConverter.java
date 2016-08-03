
package org.mpilone.hazelcastmq.camel;

import java.util.Map;

import org.apache.camel.Message;
import org.apache.camel.impl.DefaultMessage;
import org.mpilone.hazelcastmq.core.GenericMessage;

/**
 * The default implementation of a message converter that simply uses the
 * headers and body without modification.
 *
 * @author mpilone
 */
public class BasicMessageConverter implements MessageConverter {

  /**
   * Converts from a Camel message to a HzMq {@link GenericMessage} message. The
   * headers and body are simply used and not modified.
   *
   * @param camelMsg the Camel message to convert
   *
   * @return the new HzMq message
   */
  @Override
  public org.mpilone.hazelcastmq.core.Message<?> fromCamelMessage(
      Message camelMsg) {

    return new GenericMessage<>(camelMsg.getBody(), camelMsg.getHeaders());
  }

  /**
   * Converts from a HzMq message to a Camel message. The headers and body are
   * simply used and not modified.
   *
   * @param mqMsg the HzMq message to convert
   *
   * @return the new Camel message
   */
  @Override
  public Message toCamelMessage(org.mpilone.hazelcastmq.core.Message<?> mqMsg) {

    DefaultMessage camelMsg = new DefaultMessage();
    camelMsg.setHeaders((Map) mqMsg.getHeaders());
    camelMsg.setBody(mqMsg.getPayload());

    return camelMsg;
  }
}
