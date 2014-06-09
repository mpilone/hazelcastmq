
package org.mpilone.hazelcastmq.camel;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

/**
 *
 * @author mpilone
 */
public class HazelcastMQCamelComponent extends DefaultComponent {

  private HazelcastMQCamelConfig configuration;

  @Override
  protected Endpoint createEndpoint(String uri, String remaining,
      Map<String, Object> parameters) throws Exception {
    String destination = "/" + remaining.replaceAll(":", "/");

    // must copy config so we do not have side effects
    HazelcastMQCamelConfig config = getConfiguration().copy();
    // allow to configure configuration from uri parameters
    setProperties(config, parameters);

    HazelcastMQCamelEndpoint endpoint = new HazelcastMQCamelEndpoint(uri, this,
        config, destination);
    setProperties(endpoint, parameters);
    return endpoint;
  }

  public HazelcastMQCamelConfig getConfiguration() {
    return configuration;
  }

  public void setConfiguration(HazelcastMQCamelConfig configuration) {
    this.configuration = configuration;
  }
}
