package org.mpilone.hazelcastmq.example.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.mpilone.hazelcastmq.camel.*;
import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;

/**
 * An example of using the {@link HazelcastMQCamelComponent} to create a Camel
 * {@link ConsumerTemplate} to poll for messages rather than having them pushed
 * to an endpoint.
 *
 * @author mpilone
 */
public class CamelConsumerTemplate {

  /**
   * The log for this class.
   */
  private final static Logger log = LoggerFactory.getLogger(
      CamelConsumerTemplate.class);

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new CamelConsumerTemplate();
  }

  public CamelConsumerTemplate() throws Exception {

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

      // Send a message to a queue.
      try (HazelcastMQContext mqContext = mqInstance.createContext()) {
        HazelcastMQProducer mqProducer = mqContext.createProducer();
        mqProducer.send("/queue/primo.test", "Hello World!");
      }

      // Create a Camel polling consumer.
      ConsumerTemplate consumerTemplate = camelContext.createConsumerTemplate();
      String body = consumerTemplate.
          receiveBody("hazelcastmq:queue:primo.test", 2000, String.class);

      if (body == null) {
        log.warn("Did not get expected message!");
      }
      else {
        log.info("Got message on queue: " + body);
      }

      consumerTemplate.stop();
      camelContext.stop();
    }
    finally {
      // Shutdown Hazelcast.
      hazelcast.getLifecycleService().shutdown();
    }
  }
}
