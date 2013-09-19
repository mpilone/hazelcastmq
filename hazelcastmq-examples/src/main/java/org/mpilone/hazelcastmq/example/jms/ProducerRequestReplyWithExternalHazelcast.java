package org.mpilone.hazelcastmq.example.jms;

import static java.util.Arrays.asList;

import javax.jms.*;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConfig;
import org.mpilone.hazelcastmq.jms.HazelcastMQJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request/reply message to a remote Hazelcast instance
 * assuming some other application will be consuming the message and sending the
 * reply. This is useful as a simple integration test with existing endpoints.
 */
public class ProducerRequestReplyWithExternalHazelcast {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerRequestReplyWithExternalHazelcast();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerRequestReplyWithExternalHazelcast() throws JMSException {

    // Create a Hazelcast instance client.
    ClientConfig config = new ClientConfig();
    config.setAddresses(asList("127.0.0.1"));
    HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient(config);

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
      Destination requestDest = session.createQueue("foo.bar");
      Destination replyDest = session.createTemporaryQueue();

      // Create a request producer and reply consumer.
      MessageProducer producer1 = session.createProducer(requestDest);
      MessageConsumer consumer1 = session.createConsumer(replyDest);

      // Send the request.
      sendRequest(session, producer1, replyDest);

      // Process the reply.
      handleReply(session, consumer1);

      // Cleanup.
      session.close();
      connection.close();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }

  private void handleReply(Session session, MessageConsumer consumer)
      throws JMSException {
    TextMessage msg = (TextMessage) consumer.receive(5000);
    Assert.notNull(msg, "Did not get required request.");

    log.info("Got reply: " + msg.getText());
  }

  private void sendRequest(Session session, MessageProducer producer,
      Destination replyDest) throws JMSException {
    TextMessage msg = session.createTextMessage();
    msg.setJMSReplyTo(replyDest);
    msg.setText("Hello World");
    producer.send(msg);

  }
}
