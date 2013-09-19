package org.mpilone.hazelcastmq.example.jms;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConfig;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * An example of using Spring's {@link JmsTemplate} to send and receive a
 * message using the HazelcastMQ JMS provider.
 * 
 * @author mpilone
 */
public class SpringJmsTemplateOneWay {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws InterruptedException {
    new SpringJmsTemplateOneWay();
  }

  /**
   * Constructs the example.
   */
  public SpringJmsTemplateOneWay() throws InterruptedException {
    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // HazelcastMQ Instance
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);

      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // HazelcastMQJms Instance
      HazelcastMQJmsConfig mqJmsConfig = new HazelcastMQJmsConfig();
      mqJmsConfig.setHazelcastMQInstance(mqInstance);

      HazelcastMQJmsConnectionFactory connectionFactory = new HazelcastMQJmsConnectionFactory(
          mqJmsConfig);

      // Setup the JMS Template
      JmsTemplate jmsOps = new JmsTemplate(connectionFactory);
      jmsOps.setReceiveTimeout(5000);

      // Send the message.
      jmsOps.convertAndSend("foo.bar", "Hello World");

      // Simulate something interesting happening before we get around to
      // consuming the message.
      Thread.sleep(1000);

      // Receive the message.
      String msg = (String) jmsOps.receiveAndConvert("foo.bar");

      log.info("Got message: " + msg);
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }

}
