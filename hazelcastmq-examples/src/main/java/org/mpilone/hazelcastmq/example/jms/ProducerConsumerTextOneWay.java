package org.mpilone.hazelcastmq.example.jms;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConfig;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request text message and consuming the message from the
 * queue using a message listener.
 */
public class ProducerConsumerTextOneWay {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerTextOneWay();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerTextOneWay() throws JMSException, InterruptedException {

    // Hazelcast Instance.
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

      // Create a connection, session, and destinations.
      Connection connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue("foo.bar");

      // Create a producer and send a message.
      log.info("Sending message.");
      MessageProducer producer = session.createProducer(destination);
      TextMessage msg = session.createTextMessage("Hello World!");
      producer.send(msg);
      producer.close();

      // Create a consumer and receive a message.
      log.info("Receiving message.");
      MessageConsumer consumer = session.createConsumer(destination);
      msg = (TextMessage) consumer.receive(2000);

      Assert.notNull(msg, "Did not get required message.");
      log.info("Got message body: " + msg.getText());

      consumer.close();

      // Cleanup.
      session.close();
      connection.close();

      mqInstance.shutdown();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }
}
