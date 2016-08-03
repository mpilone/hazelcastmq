package org.mpilone.hazelcastmq.example.camel;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.*;
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
 * request asynchronously and do some other work while waiting for a reply.
 *
 * @author mpilone
 */
public class CamelToCamelRequestReplyAsync extends ExampleApp {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      CamelToCamelRequestReplyAsync.class);

  public static void main(String[] args) throws Exception {
    CamelToCamelRequestReplyAsync app = new CamelToCamelRequestReplyAsync();
    app.runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    BrokerConfig brokerConfig = new BrokerConfig(hazelcast);

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
              .to(ExchangePattern.InOut, "hazelcastmq:queue:primo.test");

          from("hazelcastmq:queue:primo.test")
              .process(new StaticLogProcessor("Starting work in endpoint."))
              .delay(2000)
              .process(new StaticLogProcessor("Finished work in endpoint."))
              .setBody(constant("Goodbye World!"));
        }
      });

      camelContext.start();

      // Create a Camel producer.
      ProducerTemplate camelProducer = camelContext.createProducerTemplate();
      camelProducer.start();

      // Send a message to the direct endpoint.
      Future<Object> futureResponse = camelProducer.asyncRequestBody(
          "direct:primo.test", "Hello World!");

      // Do some other work while waiting for a reply.
      String reply = null;
      for (int i = 1; i <= 20 && reply == null; i++) {
        log.info("Doing some work while waiting for a reply " + i);
        try {
          reply = (String) futureResponse.get(200, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
          // Ignore and try again.
        }
      }

      // Do some other work while we wait.
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

  private static class StaticLogProcessor implements Processor {

    private final String msg;

    public StaticLogProcessor(String msg) {
      this.msg = msg;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
      log.info(msg);
    }
  }

}
