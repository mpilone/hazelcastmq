
package org.mpilone.hazelcastmq.core;

import java.util.EventObject;

/**
 *
 * @author mpilone
 */
public class ReadReadyEvent extends EventObject {

  public ReadReadyEvent(Channel source) {
    super(source);
  }

  public Channel getChannel() {
    return (Channel) getSource();
  }

}
