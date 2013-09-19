package org.mpilone.hazelcastmq.jms;

import java.util.UUID;

class UuidIdGenerator implements IdGenerator {

  /*
   * 
   */
  @Override
  public String newId() {
    return UUID.randomUUID().toString();
  }

}
