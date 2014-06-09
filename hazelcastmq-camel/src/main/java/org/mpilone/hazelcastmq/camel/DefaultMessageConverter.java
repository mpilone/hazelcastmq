
package org.mpilone.hazelcastmq.camel;

import static java.lang.String.format;

import java.util.Map;

import org.apache.camel.Message;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.DefaultMessage;
import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

/**
 * The default implementation of a message converter.
 *
 * @author mpilone
 */
public class DefaultMessageConverter implements MessageConverter {

  /**
   * Converts from a Camel message to a HzMq message. The headers are simply
   * copied unmodified. The body is mapped by type:
   * <ul>
   * <li>String: set on the HzMq message and the content type set to
   * text/plain</li>
   * <li>byte[]: set on the HzMq message and the content type set to
   * application/octet-stream</li>
   * <li>null: set on the HzMq message with no content type</li>
   * <li>all others: exception raised</li>
   * </ul>
   *
   * @param camelMsg the Camel message to convert
   *
   * @return the new HzMq message
   */
  @Override
  public HazelcastMQMessage fromCamelMessage(Message camelMsg) {
    HazelcastMQMessage mqMsg = new HazelcastMQMessage();

    Map<String, Object> camelHeaders = camelMsg.getHeaders();

    if (camelHeaders != null) {
      for (Map.Entry<String, Object> camelHeader : camelHeaders.entrySet()) {
        if (camelHeader.getValue() instanceof String) {
          mqMsg.getHeaders().put(camelHeader.getKey(), (String) camelHeader.
              getValue());
        }
      }
    }

    Object camelBody = camelMsg.getBody();
    if (camelBody instanceof String) {
      mqMsg.setBody((String) camelBody);
      mqMsg.setContentType("text/plain");
    }
    else if (camelBody instanceof byte[]) {
      mqMsg.setBody((byte[]) camelBody);
      mqMsg.setContentType("application/octet-stream");
    }
    else if (camelBody == null) {
      mqMsg.setBody((byte[]) null);
    }
    else {
      throw new RuntimeCamelException(format(
          "Unsupported message body type: %s", camelBody.getClass().getName()));
    }

    return mqMsg;
  }

  /**
   * Converts from a Camel message to a HzMq message. The headers are simply
   * copied unmodified. The body is mapped by type:
   * <ul>
   * <li>If the content type is text/plain, the body is set as a String</li>
   * <li>all others: the body is set as a byte[] (or null)</li>
   * </ul>
   *
   * @param mqMsg the HzMq message to convert
   *
   * @return the new Camel message
   */
  @Override
  public Message toCamelMessage(HazelcastMQMessage mqMsg) {
    DefaultMessage camelMsg = new DefaultMessage();

    camelMsg.setHeaders((Map) mqMsg.getHeaders().getHeaderMap());

    if (mqMsg.getContentType() != null && mqMsg.getContentType().equals(
        "text/plain")) {
      camelMsg.setBody(mqMsg.getBodyAsString());
    }
    else {
      camelMsg.setBody(mqMsg.getBody());
    }

    return camelMsg;
  }
}
