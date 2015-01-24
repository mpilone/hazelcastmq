package org.mpilone.hazelcastmq.example.camel;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import java.util.concurrent.TimeUnit;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.mpilone.hazelcastmq.camel.*;
import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.slf4j.*;

/**
 * An example of using the {@link HazelcastMQCamelComponent} to consume a
 * message from a HzMq topic using two consumers on an endpoint.
 *
 * @author mpilone
 */
public class CamelTopicConsumers extends ExampleApp {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(CamelTopicConsumers.class);

  public static void main(String[] args) throws Exception {
    CamelTopicConsumers app = new CamelTopicConsumers();
    app.runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // Create the HazelcaseMQ instance.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);
      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // Create the camel component.
      HazelcastMQCamelConfig mqCamelConfig = new HazelcastMQCamelConfig();
      mqCamelConfig.setHazelcastMQInstance(mqInstance);

      HazelcastMQCamelComponent mqCamelComponent =
          new HazelcastMQCamelComponent();
      mqCamelComponent.setConfiguration(mqCamelConfig);

      // Create the Camel context. This could be done via a Spring XML file.
      CamelContext camelContext = new DefaultCamelContext();
      camelContext.addComponent("hazelcastmq", mqCamelComponent);

      camelContext.addRoutes(new RouteBuilder() {
        @Override
        public void configure() {
          from("hazelcastmq:topic:event.test")
              .to("hazelcastmq:queue:primo.test");
          from("hazelcastmq:topic:event.test")
              .to("hazelcastmq:queue:secondo.test");
        }
      });

      camelContext.start();

      // Send a message to the topic. The Camel route will result in two 
      // instances of the message being consumed from the topic and placed
      // into queues.
      try (HazelcastMQContext mqContext = mqInstance.createContext()) {
        HazelcastMQProducer mqProducer = mqContext.createProducer();
        mqProducer.send("/topic/event.test", "Hello World!");

        // Consume from both queues.
        try (HazelcastMQConsumer mqConsumer = mqContext.createConsumer(
            "/queue/primo.test")) {
          HazelcastMQMessage msg = mqConsumer.receive(12, TimeUnit.SECONDS);

          if (msg == null) {
            log.warn("Did not get expected message!");
          } else {
            log.info("Got expected message on primo queue.");
          }
        }

        try (HazelcastMQConsumer mqConsumer = mqContext.createConsumer("/queue/secondo.test")) {
          HazelcastMQMessage msg = mqConsumer.receive(12, TimeUnit.SECONDS);

          if (msg == null) {
            log.warn("Did not get expected message!");
          } else {
            log.info("Got expected message on secondo queue.");
          }
        }
      }
      camelContext.stop();

      mqInstance.shutdown();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.shutdown();
    }
  }
}
