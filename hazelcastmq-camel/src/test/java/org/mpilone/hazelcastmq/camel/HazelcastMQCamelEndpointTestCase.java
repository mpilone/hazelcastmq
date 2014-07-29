
package org.mpilone.hazelcastmq.camel;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

/**
 * Test case for the {@link HazelcastMQCamelEndpoint} class.
 *
 * @author mpilone
 */
public class HazelcastMQCamelEndpointTestCase {

  /**
   * Tests converting various Camel endpoint URIs to HazelcastMQ destinations.
   */
  @Test
  public void testToHazelcastMQDestination() {
    // No destination type.
    String actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination("foo.bar");
    assertEquals("/queue/foo.bar", actual);

    // Queue destination type.
    actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination("queue:foo.bar");
    assertEquals("/queue/foo.bar", actual);

    // Topic destination type.
    actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination("topic:foo.bar");
    assertEquals("/topic/foo.bar", actual);

    // Queue destination type in HzMq format.
    actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination("/queue/foo.bar");
    assertEquals("/queue/foo.bar", actual);

    // Topic destination type in HzMq format.
    actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination("/topic/foo.bar");
    assertEquals("/topic/foo.bar", actual);

    // Queue destination type with extra, leading colon.
    actual = HazelcastMQCamelEndpoint.toHazelcastMQDestination(":queue:foo.bar");
    assertEquals("/queue/foo.bar", actual);
  }

}
