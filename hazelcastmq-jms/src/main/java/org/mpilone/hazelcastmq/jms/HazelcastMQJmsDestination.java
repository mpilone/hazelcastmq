package org.mpilone.hazelcastmq.jms;

import javax.jms.Destination;

class HazelcastMQJmsDestination implements Destination {

  private String destinationName;

  private String destinationPrefix;

  /**
   * @param destinationName
   * @param destinationPrefix
   */
  public HazelcastMQJmsDestination(String destinationPrefix,
      String destinationName) {
    super();
    this.destinationName = destinationName;
    this.destinationPrefix = destinationPrefix;
  }

  String getDestinationPrefix() {
    return destinationPrefix;
  }

  String getDestinationName() {
    return destinationName;
  }

  String getMqName() {
    return destinationPrefix + destinationName;
  }

}
