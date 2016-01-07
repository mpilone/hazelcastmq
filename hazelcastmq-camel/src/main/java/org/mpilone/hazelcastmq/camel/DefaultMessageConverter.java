
package org.mpilone.hazelcastmq.camel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.camel.Message;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.DefaultMessage;
import org.mpilone.hazelcastmq.core.HazelcastMQMessage;

import static java.lang.String.format;

/**
 * The default implementation of a message converter.
 *
 * @author mpilone
 */
public class DefaultMessageConverter implements MessageConverter {

  private static final String PLAIN = "text/plain";
  private static final String BINARY = "application/octet-stream";
  private static final String SERIAL = "application/x-java-serialized-object";

  /**
   * Converts from a Camel message to a HzMq message. The headers are simply
   * copied unmodified. The body is mapped by type:
   * <ul>
   * <li>String: set on the HzMq message and the content type set to
   * {@value #PLAIN}</li>
   * <li>byte[]: set on the HzMq message and the content type set to
   * {@value #BINARY}</li>
   * <li>Serializable: serialized to a byte[] and set on the HzMq
   * message and the content type set to {@value #SERIAL}</li>
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
      mqMsg.setContentType(PLAIN);
    }
    else if (camelBody instanceof byte[]) {
      mqMsg.setBody((byte[]) camelBody);
      mqMsg.setContentType(BINARY);
    }
    else if (camelBody instanceof Serializable) {
      mqMsg.setBody(serialize(camelBody));
      mqMsg.setContentType(SERIAL);
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
   * <li>If the content type is {@value #PLAIN}, the body is set as a String</li>
   * <li>If the content type is {@value #SERIAL}, the body is deserialized and
   * set as an Object</li>
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

    String contentType = mqMsg.getContentType();
    if (PLAIN.equals(contentType)) {
      camelMsg.setBody(mqMsg.getBodyAsString());
    }
    else if (SERIAL.equals(contentType)) {
      camelMsg.setBody(deserialize(mqMsg.getBody()));
    }
    else {
      camelMsg.setBody(mqMsg.getBody());
    }

    return camelMsg;
  }

  private static byte[] serialize(Object object) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ObjectOutput oo = new ObjectOutputStream(out)) {
      oo.writeObject(object);
    } catch (IOException e) {
      throw new RuntimeCamelException(format(
          "Could not serialize message body type: %s",
          object.getClass().getName()), e);
    }
    return out.toByteArray();
  }

  private static Object deserialize(byte[] b) {
    try (ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(b))) {
      return in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeCamelException(e);
    }
  }
}
