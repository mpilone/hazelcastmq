package org.mpilone.hazelcastmq.example;

import javax.jms.*;

import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a request text message and consuming the message from the
 * queue using a message listener.
 */
public class ProducerConsumerTextOneWayMessageListener implements
    MessageListener {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private boolean msgReceived = false;

  public static void main(String[] args) throws Exception {
    new ProducerConsumerTextOneWayMessageListener();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerTextOneWayMessageListener() throws JMSException,
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
      Destination requestDest = session.createQueue("foo.bar");

      // Create a request producer and reply consumer.
      MessageProducer producer1 = session.createProducer(requestDest);
      MessageConsumer consumer1 = session.createConsumer(requestDest);
      consumer1.setMessageListener(this);

      TextMessage msg = session.createTextMessage("Hello World!");
      producer1.send(msg);

      // Wait for the consumer to receive it.
      Thread.sleep(1000);
      Assert.isTrue(msgReceived, "Did not get required message.");

      producer1.close();
      consumer1.close();

      // Cleanup.
      session.close();
      connection.close();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
   */
  @Override
  public void onMessage(Message msg) {
    TextMessage textMsg = (TextMessage) msg;

    try {
      log.info("Got message: " + textMsg.getText());
      msgReceived = true;
    }
    catch (Exception ex) {
      // Ignore
    }
  }
}
