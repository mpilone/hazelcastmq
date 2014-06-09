
package org.mpilone.hazelcastmq.camel;

import org.apache.camel.RuntimeCamelException;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;

/**
 *
 * @author mpilone
 */
public class HazelcastMQCamelConfig implements Cloneable {

  private HazelcastMQInstance hazelcastMQInstance;
  private MessageConverter messageConverter = new DefaultMessageConverter();

  public void setHazelcastMQInstance(HazelcastMQInstance hazelcastMQInstance) {
    this.hazelcastMQInstance = hazelcastMQInstance;
  }

  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  public MessageConverter getMessageConverter() {
    return messageConverter;
  }

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
}
