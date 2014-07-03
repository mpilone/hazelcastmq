package org.mpilone.hazelcastmq.example.camel;

import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.mpilone.hazelcastmq.camel.*;
import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * An example of using the {@link HazelcastMQCamelComponent} to produce messages
 * to specific destinations per messages.
 *
 * @author mpilone
 */
public class CamelToCamelOverrideDestination {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      CamelToCamelOverrideDestination.class);

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new CamelToCamelOverrideDestination();
  }

  public CamelToCamelOverrideDestination() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
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

      camelContext.start();

      // Send messages to message specific queues, overrideing the destination
      // configured in the endpoint.
      ProducerTemplate camelProducer = camelContext.createProducerTemplate();
      camelProducer.sendBodyAndHeader("hazelcastmq:queue:dummy", "Hello World!",
          "CamelHzMqDestination", "queue:primo.test");
      camelProducer.sendBodyAndHeader("hazelcastmq:queue:dummy",
          "Goodbye World!", "CamelHzMqDestination", "queue:secondo.test");

      // Try to receive the messages from the various message specific queues.
      try (HazelcastMQContext mqContext = mqInstance.createContext()) {

        try (HazelcastMQConsumer mqConsumer =
            mqContext.createConsumer("/queue/primo.test")) {
          HazelcastMQMessage msg = mqConsumer.receive(2, TimeUnit.SECONDS);
          assertMessage(msg);
        }

        try (HazelcastMQConsumer mqConsumer =
            mqContext.createConsumer("/queue/secondo.test")) {
          HazelcastMQMessage msg = mqConsumer.receive(2, TimeUnit.SECONDS);
          assertMessage(msg);
        }
      }
      camelContext.stop();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }
  }

  private void assertMessage(HazelcastMQMessage msg) {
    if (msg == null) {
      log.warn("Did not get expected message!");
    }
    else {
      log.info("Got message: " + msg.getBodyAsString());
    }
  }
}
