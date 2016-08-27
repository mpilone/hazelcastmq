
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

  /**
   * Constructs the read-ready event.
   *
   * @param source the channel that is read-ready
   */
  public ReadReadyEvent(Channel source) {
    super(source);
  }

  /**
   * Returns the channel that is read-ready.
   *
   * @return the read-ready channel
   */
  public Channel getChannel() {
    return (Channel) getSource();
  }

}
