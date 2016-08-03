package org.mpilone.hazelcastmq.camel;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.mpilone.hazelcastmq.core.DataStructureKey;
import org.mpilone.hazelcastmq.core.HazelcastMQ;

/**
 * <p>
 * An Apache Camel component for creating endpoints to {@link HazelcastMQ}
 * channels. The endpoint URI can specify the destination service name such as
 * {@code hazelcastmq:queue:my.outgoing.orders} or
 * {@code hazelcastmq:ringbuffer:order.processed.event}. The destination will be
 * transformed into a standard HzMq channel. If a service name is not specified,
 * a queue channel will be assumed.
 * </p>
 * <p>
 * Refer to the {@link CamelConfig} class for details on configuration and
 * supported URL parameters.
 * </p>
 *
 * @author mpilone
 */
public class CamelComponent extends DefaultComponent {

  private CamelConfig configuration;

  /**
   * Constructs the component with no configuration. A default configuration
   * will be used if one is not set before adding the component to the context.
   */
  public CamelComponent() {
  }

  /**
   * Constructs the component with the given configuration.
   *
   * @param configuration the component configuration
   */
  public CamelComponent(CamelConfig configuration) {
    this.configuration = configuration;
  }

  @Override
  protected Endpoint createEndpoint(String uri, String remaining,
      Map<String, Object> parameters) throws Exception {

    // Must copy config so we do not have side effects.
    CamelConfig config = getConfiguration().copy();

    // Allow to configure configuration from uri parameters.
    setProperties(config, parameters);

    // Create and return the endpoint.
    CamelEndpoint endpoint = new CamelEndpoint(uri, this, config, remaining);
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
  public CamelConfig getConfiguration() {
    return configuration;
  }

  /**
   * Sets the configuration for this component. The component configuration
   * serves as the defaults for all endpoints. Endpoints can override these
   * values using URL parameters.
   *
   * @param configuration the component configuration
   */
  public void setConfiguration(CamelConfig configuration) {
    this.configuration = configuration;
  }

  @Override
  public void start() throws Exception {
    if (configuration == null) {
      this.configuration = new CamelConfig();
    }

    super.start();
  }

  private DataStructureKey toDataStructureKey(String remaining) {
    // TODO: Implement method
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
