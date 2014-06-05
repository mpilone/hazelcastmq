package org.mpilone.hazelcastmq.jms;

import javax.jms.Destination;

/**
 * A JMS destination with a name and a prefix. The name is the queue or topic
 * name in Hazelcast while the prefix indicates if the destination is a queue or
 * topic such as "/queue/" or "/topic/".
 *
 * @author mpilone
 */
class HazelcastMQJmsDestination implements Destination {

  private final String destinationName;

  private final String destinationPrefix;

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
