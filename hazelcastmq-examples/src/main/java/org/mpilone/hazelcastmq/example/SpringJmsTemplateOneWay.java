package org.mpilone.hazelcastmq.example;

import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
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
      // Setup the connection factory.
      HazelcastMQConnectionFactory connectionFactory = new HazelcastMQConnectionFactory();
      connectionFactory.setHazelcast(hazelcast);

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
