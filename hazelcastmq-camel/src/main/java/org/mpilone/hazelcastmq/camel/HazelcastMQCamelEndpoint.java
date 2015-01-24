
package org.mpilone.hazelcastmq.camel;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultEndpoint;
import org.mpilone.hazelcastmq.core.HazelcastMQ;

/**
 * <p>
 * An Apache Camel endpoint for creating consumers and producers on
 * {@link HazelcastMQ} destinations.
 * </p>
 * <p>
 * Refer to the {@link HazelcastMQCamelConfig} class for details on
 * configuration and supported URL parameters.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQCamelEndpoint extends DefaultEndpoint implements
    MultipleConsumersSupport {
  private final String destination;
  private final HazelcastMQCamelConfig configuration;

  /**
   * Constructs the endpoint.
   *
   * @param uri the original endpoint URI
   * @param component the component that this endpoint belongs to
   * @param config the endpoint specific configuration
   * @param destination the HzMq message destination for all producers and
   * consumers created by this endpoint
   */
  HazelcastMQCamelEndpoint(String uri, HazelcastMQCamelComponent component,
      HazelcastMQCamelConfig config, String destination) {
    super(uri, component);

    this.destination = destination;
    this.configuration = config;
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    super.doShutdown();
  }

  @Override
  public Producer createProducer() throws Exception {
    return new HazelcastMQCamelProducer(this);
  }

  @Override
  public boolean isMultipleConsumersSupported() {
    // Allow multiple consumers on both queues and topics
    return true;
  }

  @Override
  public Consumer createConsumer(Processor prcsr) throws Exception {
    Consumer c = new HazelcastMQCamelConsumer(this, prcsr);
    configureConsumer(c);
    return c;
  }

  @Override
  public PollingConsumer createPollingConsumer() throws Exception {
    PollingConsumer pc = new HazelcastMQCamelPollingConsumer(this);
    configurePollingConsumer(pc);
    return pc;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  /**
   * Returns the endpoint specific configuration.
   *
   * @return the configuration
   */
  HazelcastMQCamelConfig getConfiguration() {
    return configuration;
  }

  /**
   * Returns the HzMq message destination for all producers and consumers
   * created by this endpoint.
   *
   * @return the destination
   */
  String getDestination() {
    return destination;
  }

  /**
   * Converts a camel destination to a HazelcastMQ destination. For example, a
   * destination of {@code queue:foo.bar} will be converted to
   * {@code /queue/foo.bar}. If queue or topic is not specified, a queue will be
   * used by default.
   *
   * @param camelDest the camel destination to convert
   *
   * @return the HazelcastMQ destination
   */
  public static String toHazelcastMQDestination(String camelDest) {
    String destination = camelDest.replaceAll(":", "/");

    if (!destination.startsWith("/")) {
      destination = "/" + destination;
    }

    if (!destination.startsWith(
        org.mpilone.hazelcastmq.core.Headers.DESTINATION_QUEUE_PREFIX)
        && !destination.startsWith(
            org.mpilone.hazelcastmq.core.Headers.DESTINATION_TOPIC_PREFIX)
        && !destination.startsWith(
            org.mpilone.hazelcastmq.core.Headers.DESTINATION_TEMPORARY_QUEUE_PREFIX)
        && !destination.startsWith(
            org.mpilone.hazelcastmq.core.Headers.DESTINATION_TEMPORARY_TOPIC_PREFIX)) {

      // Default to a queue if no destination prefix was specifed.
      destination =
          org.mpilone.hazelcastmq.core.Headers.DESTINATION_QUEUE_PREFIX
          + camelDest;
    }

    return destination;
  }

}
