
package org.mpilone.hazelcastmq.camel;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.mpilone.hazelcastmq.core.HazelcastMQContext;

/**
 *
 * @author mpilone
 */
public class HazelcastMQCamelEndpoint extends DefaultEndpoint {
  private final String destination;
  private final HazelcastMQCamelConfig configuration;
  private final HazelcastMQContext mqContext;

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
    return new HazelcastMQCamelConsumer(this, prcsr);
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  HazelcastMQCamelConfig getConfiguration() {
    return configuration;
  }

  String getDestination() {
    return destination;
  }

}
