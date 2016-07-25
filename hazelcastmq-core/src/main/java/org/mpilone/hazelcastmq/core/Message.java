
package org.mpilone.hazelcastmq.core;

/**
 *
 * @author mpilone
 * @param <B> the type of the body of the message
 */
public interface Message<B> {

  MessageHeaders getHeaders();

  B getBody();
}
