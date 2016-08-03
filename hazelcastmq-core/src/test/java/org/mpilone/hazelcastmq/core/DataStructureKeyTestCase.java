
package org.mpilone.hazelcastmq.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.hazelcast.collection.impl.queue.QueueService;

/**
 * Test case for the {@link DataStructureKey} class.
 *
 * @author mpilone
 */
public class DataStructureKeyTestCase {

  /**
   * Tests converting various Camel endpoint URIs to HazelcastMQ destinations.
   */
  @Test
  public void testToHazelcastMQDestination() {
    // No destination type.
    DataStructureKey actual = DataStructureKey.fromString("foo.bar");
    assertEquals(QueueService.SERVICE_NAME, actual.getServiceName());
    assertEquals("foo.bar", actual.getName());

    // Queue destination type.
    actual = DataStructureKey.fromString("queue:foo.bar");
    assertEquals(QueueService.SERVICE_NAME, actual.getServiceName());
    assertEquals("foo.bar", actual.getName());

    // Queue destination type.
    actual = DataStructureKey.fromString(":queue:foo.bar");
    assertEquals(QueueService.SERVICE_NAME, actual.getServiceName());
    assertEquals("foo.bar", actual.getName());

    // Queue destination type in / format.
    actual = DataStructureKey.fromString("/queue/foo.bar");
    assertEquals(QueueService.SERVICE_NAME, actual.getServiceName());
    assertEquals("foo.bar", actual.getName());

  }

}
