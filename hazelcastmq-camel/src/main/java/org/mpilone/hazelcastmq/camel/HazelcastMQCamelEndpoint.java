
package org.mpilone.hazelcastmq.camel;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultEndpoint;
import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQContext;

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
public class HazelcastMQCamelEndpoint extends DefaultEndpoint {
  private final String destination;
  private final HazelcastMQCamelConfig configuration;
  private final HazelcastMQContext mqContext;

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
    this.mqContext = configuration.getHazelcastMQInstance().createContext();
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();

    mqContext.start();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();

      mqContext.stop();
  }

  @Override
  protected void doShutdown() throws Exception {
    super.doShutdown();

    mqContext.close();
  }

  @Override
  public Producer createProducer() throws Exception {
    return new HazelcastMQCamelProducer(this);
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

}
