package org.mpilone.hazelcastmq.example;

import javax.jms.*;

import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request message to a local Hazelcast instance while
 * acting as both the producer and consumer of the message. The message will
 * have an expiration time that will pass before the consumer gets to the
 * message.
 */
public class ProducerConsumerMessageExpiration {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerMessageExpiration();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerMessageExpiration() throws JMSException,
      InterruptedException {

    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConnectionFactory connectionFactory = new HazelcastMQConnectionFactory();
      connectionFactory.setHazelcast(hazelcast);

      // Create a connection, session, and destinations.
      Connection connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Destination requestDest = session.createQueue("example.test1");

      // Create a request producer (node A).
      MessageProducer producer1 = session.createProducer(requestDest);

      // Create a request consumer (node B).
      MessageConsumer consumer2 = session.createConsumer(requestDest);

      // Produce and consume message
      sendRequest(session, producer1);

      // Simulate a long wait before the consumer gets the message
      Thread.sleep(5000);

      handleRequest(session, consumer2);

      // Cleanup
      session.close();
      connection.close();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }

  private void handleRequest(Session session, MessageConsumer consumer)
      throws JMSException {

    TextMessage msg = (TextMessage) consumer.receive(1000);

    if (msg != null) {
      log.info("Got request: " + msg.getText());
    }
    else {
      log.info("Did not get a request (expected behavior).");
    }
  }

  private void sendRequest(Session session, MessageProducer producer)
      throws JMSException {
    TextMessage msg = session.createTextMessage();

    // Make the message expire if not received in 2 seconds.
    msg.setJMSExpiration(System.currentTimeMillis() + 2000);
    msg.setText("Some work unit.");
    producer.send(msg);

  }
}
