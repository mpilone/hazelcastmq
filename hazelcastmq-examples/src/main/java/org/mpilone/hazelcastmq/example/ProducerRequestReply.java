package org.mpilone.hazelcastmq.example;

import static java.util.Arrays.asList;

import javax.jms.*;

import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request/reply message to a remote Hazelcast instance
 * assuming some other application will be consuming the message and sending the
 * reply.
 */
public class ProducerRequestReply {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerRequestReply();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerRequestReply() throws JMSException {

    // Create a Hazelcast instance client.
    ClientConfig config = new ClientConfig();
    config.setAddresses(asList("127.0.0.1"));
    HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient(config);

    try {
      // Setup the connection factory.
      HazelcastMQConnectionFactory connectionFactory = new HazelcastMQConnectionFactory();
      connectionFactory.setHazelcast(hazelcast);

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
