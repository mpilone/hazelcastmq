package org.mpilone.hazelcastmq.example.jms;

import java.util.Arrays;

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
 * Example of sending a request bytes message and consuming the message from the
 * queue.
 */
public class ProducerConsumerBytesOneWay {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new ProducerConsumerBytesOneWay();
  }

  /**
   * Constructs the example.
   * 
   * @throws JMSException
   */
  public ProducerConsumerBytesOneWay() throws JMSException {

    // Hazelcast Instance
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
      Destination requestDest = session.createQueue("foo.bar");

      // Create a request producer and reply consumer.
      log.info("Sending message.");
      MessageProducer producer = session.createProducer(requestDest);

      byte[] data = new byte[] { 8, 6, 7, 5, 3, 0, 9 };

      BytesMessage msg = session.createBytesMessage();
      msg.writeBytes(data);
      producer.send(msg);
      producer.close();

      log.info("Receiving message.");
      MessageConsumer consumer = session.createConsumer(requestDest);
      msg = (BytesMessage) consumer.receive(2000);

      Assert.notNull(msg, "Did not get required message.");

      data = new byte[(int) msg.getBodyLength()];
      msg.readBytes(data);

      log.info("Got message body: " + Arrays.toString(data));

      consumer.close();

      // Cleanup.
      session.close();
      connection.close();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }
  }
}
