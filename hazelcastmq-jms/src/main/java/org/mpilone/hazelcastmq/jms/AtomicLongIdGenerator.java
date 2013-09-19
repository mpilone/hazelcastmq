package org.mpilone.hazelcastmq.jms;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongIdGenerator implements IdGenerator {

  private AtomicLong largestId = new AtomicLong();

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.jms.IdGenerator#newId()
   */
  @Override
  public String newId() {
    return String.valueOf(largestId.incrementAndGet());
  }

}
