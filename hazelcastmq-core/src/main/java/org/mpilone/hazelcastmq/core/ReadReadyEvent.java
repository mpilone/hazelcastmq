
package org.mpilone.hazelcastmq.core;

import java.util.EventObject;

/**
 * Event fired when a channel is ready for reading. The event indicates which
 * channel is ready for reading.
 *
 * @author mpilone
 */
public class ReadReadyEvent extends EventObject {
  private static final long serialVersionUID = 1L;

  public ReadReadyEvent(Channel source) {
    super(source);
  }

  public Channel getChannel() {
    return (Channel) getSource();
  }

}
