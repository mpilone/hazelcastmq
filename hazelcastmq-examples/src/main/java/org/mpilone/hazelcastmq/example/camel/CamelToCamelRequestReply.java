package org.mpilone.hazelcastmq.example.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.mpilone.hazelcastmq.camel.*;
import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * An example of using the {@link CamelComponent} to produce a
 * request and wait for a reply from the other end.
 *
 * @author mpilone
 */
public class CamelToCamelRequestReply extends ExampleApp {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      CamelToCamelRequestReply.class);

  public static void main(String[] args) throws Exception {
    CamelToCamelRequestReply app = new CamelToCamelRequestReply();
    app.runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcaseMQ broker.
    BrokerConfig brokerConfig = new BrokerConfig();
    brokerConfig.setHazelcastInstance(hazelcast);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Create the camel component.
      CamelConfig mqCamelConfig = new CamelConfig();
      mqCamelConfig.setBroker(broker);

      CamelComponent mqCamelComponent =          new CamelComponent();
      mqCamelComponent.setConfiguration(mqCamelConfig);

      // Create the Camel context. This could be done via a Spring XML file.
      CamelContext camelContext = new DefaultCamelContext();
      camelContext.addComponent("hazelcastmq", mqCamelComponent);

      camelContext.addRoutes(new RouteBuilder() {
        @Override
        public void configure() {
          from("direct:primo.test")
              .to(ExchangePattern.InOut, "hazelcastmq:queue:secondo.test");

          from("hazelcastmq:queue:secondo.test").delay(1500)
              .setBody(constant("Goodbye World!"));
        }
      });

      camelContext.start();

      // Create a Camel producer.
      ProducerTemplate camelProducer = camelContext.createProducerTemplate();
      camelProducer.start();

      // Send a message to the direct endpoint.
      String reply = (String) camelProducer.sendBody("direct:primo.test",
          ExchangePattern.InOut, "Hello World!");

      if (reply == null) {
        log.warn("Did not get expected message!");
      }
      else {
        log.info("Got reply message: " + reply);
      }

      camelContext.stop();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.shutdown();
    }
  }
}
