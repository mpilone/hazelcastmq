
package org.mpilone.hazelcastmq.camel;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.mpilone.hazelcastmq.core.HazelcastMQ;

/**
 * <p>
 * An Apache Camel component for creating endpoints to {@link HazelcastMQ}
 * queues and topics. The endpoint URI can specify if the destination is a queue
 * or a topic such as {@code hazelcastmq:queue:my.outgoing.orders} or
 * {@code hazelcastmq:topic:order.processed.event}. The destination will be
 * transformed into a standard HzMq destination such as
 * {@code /queue/my.outgoing.orders} or {@code /topic/order.processed.event}. If
 * queue or topic is not specified, a queue will be assumed.
 * </p>
 * <p>
 * Refer to the {@link HazelcastMQCamelConfig} class for details on
 * configuration and supported URL parameters.
 * </p>
 *
 * @author mpilone
 */
public class HazelcastMQCamelComponent extends DefaultComponent {

  private HazelcastMQCamelConfig configuration;

  @Override
  protected Endpoint createEndpoint(String uri, String remaining,
      Map<String, Object> parameters) throws Exception {
    String destination = HazelcastMQCamelEndpoint.toHazelcastMQDestination(
        remaining);

    // Must copy config so we do not have side effects.
    HazelcastMQCamelConfig config = getConfiguration().copy();

    // Allow to configure configuration from uri parameters.
    setProperties(config, parameters);

    // Create and return the endpoint.
    HazelcastMQCamelEndpoint endpoint = new HazelcastMQCamelEndpoint(uri, this,
        config, destination);
    setProperties(endpoint, parameters);
    return endpoint;
  }

  /**
   * Returns the configuration for this component. The component configuration
   * serves as the defaults for all endpoints. Endpoints can override these
   * values using URL parameters.
   *
   * @return the component configuration
   */
  public HazelcastMQCamelConfig getConfiguration() {
    return configuration;
  }

  /**
   * Sets the configuration for this component. The component configuration
   * serves as the defaults for all endpoints. Endpoints can override these
   * values using URL parameters.
   *
   * @param configuration the component configuration
   */
  public void setConfiguration(HazelcastMQCamelConfig configuration) {
    this.configuration = configuration;
  }
}
