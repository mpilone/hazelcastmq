package org.mpilone.hazelcastmq.example.jms;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConfig;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request/reply message to a local Hazelcast instance
 * while acting as both the producer and consumer of the message.
 */
public class ProducerConsumerRequestReply {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerRequestReply();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerRequestReply() throws JMSException {

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

      // Create a connection, session, and destinations.
      Connection connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Destination requestDest = session.createQueue("example.test1");
      Destination replyDest = session.createTemporaryQueue();

      // Create a request producer and reply consumer (node A).
      MessageProducer producer1 = session.createProducer(requestDest);
      MessageConsumer consumer1 = session.createConsumer(replyDest);

      // Create a request consumer and a reply producer (node B).
      MessageProducer producer2 = session.createProducer(replyDest);
      MessageConsumer consumer2 = session.createConsumer(requestDest);

      // Produce and consume 10 messages.
      for (int i = 0; i < 10; ++i) {
        sendRequest(session, producer1, replyDest);
        handleRequest(session, consumer2, producer2);
        handleReply(session, consumer1);
      }

      // Cleanup
      session.close();
      connection.close();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }

  private void handleReply(Session session, MessageConsumer consumer)
      throws JMSException {
    TextMessage msg = (TextMessage) consumer.receive(1000);
    log.info("Got reply: " + msg.getText());
  }

  private void handleRequest(Session session, MessageConsumer consumer,
      MessageProducer producer) throws JMSException {
    TextMessage msg = (TextMessage) consumer.receive(1000);
    log.info("Got request: " + msg.getText());

    Destination replyDest = msg.getJMSReplyTo();
    String correlationId = msg.getJMSCorrelationID();

    msg = session.createTextMessage("Pong");
    msg.setJMSDestination(replyDest);
    msg.setJMSCorrelationID(correlationId);
    producer.send(msg);
  }

  private void sendRequest(Session session, MessageProducer producer,
      Destination replyDest) throws JMSException {
    TextMessage msg = session.createTextMessage();
    msg.setJMSReplyTo(replyDest);
    msg.setText("Ping");
    producer.send(msg);

  }
}
