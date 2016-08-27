
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;

/**
 * The core interface of messages that can be sent through HazelcastMQ.
 *
 * @author mpilone
 * @param <P> the type of the body of the message
 */
public interface Message<P> extends Serializable {

  /**
   * Returns the message headers.
   *
   * @return the message headers
   */
  MessageHeaders getHeaders();

  /**
   * Returns the message payload.
   *
   * @return the message payload
   */
  P getPayload();
}
